package queue

import (
	"fmt"
	"sync"
	"time"
)

// deliveryState 记录一个任务在某个消费组内的投递状态。
type deliveryState struct {
	Status      DeliveryStatus `json:"status"`
	Attempts    int            `json:"attempts"`      // 已投递次数
	NextRetryAt time.Time      `json:"next_retry_at"` // StatusRetry 时生效
	LeaseUntil  time.Time      `json:"lease_until"`   // StatusInflight 时生效
	LastFail    FailKind       `json:"last_fail"`
	LastError   string         `json:"last_error"`
	DeadAt      time.Time      `json:"dead_at"`
}

// snapshot 是持久化的完整状态。
type snapshot struct {
	NextSeq int64                            `json:"next_seq"`
	Jobs    []Job                            `json:"jobs"`
	Groups  map[string]map[string]deliveryState `json:"groups"` // group -> jobID -> state
	Dedup   []dedupEntry                     `json:"dedup"`
}

// Queue 是本地持久化任务队列。
//
// 投递语义：至少一次。Pull 把任务置为 inflight 并带上租约，
// 租约超时未 Ack 会自动变得可再次投递；Ack 幂等；
// 进程重启时未确认的 inflight 一律回到 pending 重新投递，
// 已 Ack 的任务绝不再投递。
type Queue struct {
	mu     sync.Mutex
	opts   Options
	st     *store
	jobs   map[string]*Job
	order  []string                            // 全局入队顺序（所有组共享同一任务序列）
	groups map[string]map[string]*deliveryState // group -> jobID -> state
	dedup  *dedupIndex
	nextSeq int64
	closed  bool
}

// Open 打开（或创建）一个队列，并从磁盘恢复状态。
// 恢复时把上次进程遗留的 inflight 重置为 pending，保证未确认任务被重新投递。
func Open(opts Options) (*Queue, error) {
	o := opts
	o.withDefaults()
	q := &Queue{
		opts:   o,
		st:     newStore(o.Dir),
		jobs:   make(map[string]*Job),
		groups: make(map[string]map[string]*deliveryState),
		dedup:  newDedupIndex(o.DedupCapacity),
	}
	snap, err := q.st.load()
	if err != nil {
		return nil, err
	}
	if snap != nil {
		q.nextSeq = snap.NextSeq
		for i := range snap.Jobs {
			job := snap.Jobs[i]
			q.jobs[job.ID] = &job
			q.order = append(q.order, job.ID)
		}
		for g, states := range snap.Groups {
			gs := make(map[string]*deliveryState, len(states))
			for id, s := range states {
				st := s
				if st.Status == StatusInflight {
					// 进程重启后租约随之失效，回到待投递。
					st.Status = StatusPending
					st.LeaseUntil = time.Time{}
				}
				cp := st
				gs[id] = &cp
			}
			q.groups[g] = gs
		}
		q.dedup.restore(snap.Dedup)
	}
	return q, nil
}

// Enqueue 入队一个任务。DedupKey 命中去重索引时返回已存在的 jobID 与 inserted=false，
// 同一个 DedupKey 不会产生第二条任务（幂等去重）。
func (q *Queue) Enqueue(job Job) (jobID string, inserted bool, err error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return "", false, ErrQueueClosed
	}
	if job.DedupKey == "" {
		return "", false, ErrEmptyDedupKey
	}
	if job.MaxAttempts < 0 {
		return "", false, ErrInvalidMaxTry
	}
	if id, ok := q.dedup.lookup(job.DedupKey); ok {
		return id, false, nil
	}
	if job.MaxAttempts == 0 {
		job.MaxAttempts = q.opts.MaxAttempts
	}
	q.nextSeq++
	job.ID = fmt.Sprintf("job-%06d", q.nextSeq)
	job.CreatedAt = time.Now()
	if err := q.dedup.add(job.DedupKey, job.ID, q.evictable); err != nil {
		return "", false, err
	}
	j := job
	q.jobs[job.ID] = &j
	q.order = append(q.order, job.ID)
	if err := q.persistLocked(); err != nil {
		return "", false, err
	}
	return job.ID, true, nil
}

// evictable 判定一个去重键是否允许淘汰：
// 仅当对应任务在所有消费组都已到达终态（done/dead）时才允许，
// 仍在 pending/inflight/retry 的任务不可淘汰。
func (q *Queue) evictable(key string) bool {
	id, ok := q.dedup.peek(key) // 只读，不能在淘汰扫描中改变 LRU 顺序
	if !ok {
		return true
	}
	for _, gs := range q.groups {
		st, ok := gs[id]
		if !ok {
			return false // 该组还没消费过此任务，视为 pending
		}
		if st.Status != StatusDone && st.Status != StatusDead {
			return false
		}
	}
	return true
}

// Pull 为指定消费组拉取一个可投递任务；没有可投递任务时返回 nil, nil。
// 同一任务在租约内只会落到同组一个消费者手里；不同组各自独立投递全量任务。
func (q *Queue) Pull(group string) (*Delivery, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, ErrQueueClosed
	}
	now := time.Now()
	gs := q.groupLocked(group)
	for _, id := range q.order {
		st := gs[id]
		if st == nil {
			st = &deliveryState{Status: StatusPending}
			gs[id] = st
		}
		deliverable := false
		switch st.Status {
		case StatusPending:
			deliverable = true
		case StatusRetry:
			deliverable = !st.NextRetryAt.After(now)
		case StatusInflight:
			deliverable = !st.LeaseUntil.After(now) // 租约超时，重新投递
		}
		if !deliverable {
			continue
		}
		st.Status = StatusInflight
		st.Attempts++
		st.LeaseUntil = now.Add(q.opts.Lease)
		if err := q.persistLocked(); err != nil {
			return nil, err
		}
		return &Delivery{
			Job:     *q.jobs[id],
			Group:   group,
			Attempts: st.Attempts,
			Status:  StatusInflight,
		}, nil
	}
	return nil, nil
}

// Ack 确认一次投递。重复确认是安全的（幂等）；确认后任务在该组绝不再投递。
func (q *Queue) Ack(group, jobID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrQueueClosed
	}
	gs := q.groupLocked(group)
	st := gs[jobID]
	if st == nil {
		if _, ok := q.jobs[jobID]; !ok {
			return ErrJobNotFound
		}
		return ErrNotInflight // 任务存在但从未投递给该组
	}
	if st.Status == StatusDone {
		return nil // 重复确认，幂等成功
	}
	if st.Status != StatusInflight {
		return ErrNotInflight
	}
	st.Status = StatusDone
	st.LeaseUntil = time.Time{}
	return q.persistLocked()
}

// Nack 上报失败：
//   - FailRetryable：若已达最大投递次数则进死信，否则按指数退避安排重投；
//   - FailFatal：直接进死信。
func (q *Queue) Nack(group, jobID string, kind FailKind, cause error) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrQueueClosed
	}
	gs := q.groupLocked(group)
	st := gs[jobID]
	if st == nil {
		if _, ok := q.jobs[jobID]; !ok {
			return ErrJobNotFound
		}
		return ErrNotInflight // 任务存在但从未投递给该组
	}
	if st.Status != StatusInflight {
		return ErrNotInflight
	}
	job := q.jobs[jobID]
	st.LastFail = kind
	if cause != nil {
		st.LastError = cause.Error()
	}
	st.LeaseUntil = time.Time{}
	if kind == FailFatal || st.Attempts >= job.MaxAttempts {
		st.Status = StatusDead
		st.DeadAt = time.Now()
	} else {
		st.Status = StatusRetry
		st.NextRetryAt = time.Now().Add(backoff(q.opts.BackoffBase, q.opts.BackoffMax, st.Attempts))
	}
	return q.persistLocked()
}

// DeadLetters 返回指定组的全部死信，按入队顺序排列。
func (q *Queue) DeadLetters(group string) []DeadLetter {
	q.mu.Lock()
	defer q.mu.Unlock()
	var out []DeadLetter
	gs := q.groups[group]
	for _, id := range q.order {
		st := gs[id]
		if st == nil || st.Status != StatusDead {
			continue
		}
		out = append(out, DeadLetter{
			Job:       *q.jobs[id],
			Group:     group,
			Attempts:  st.Attempts,
			LastFail:  st.LastFail,
			LastError: st.LastError,
			DeadAt:    st.DeadAt,
		})
	}
	return out
}

// Close 落盘并关闭队列。
func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil
	}
	q.closed = true
	return q.persistLocked()
}

func (q *Queue) groupLocked(group string) map[string]*deliveryState {
	gs, ok := q.groups[group]
	if !ok {
		gs = make(map[string]*deliveryState)
		q.groups[group] = gs
	}
	return gs
}

func (q *Queue) persistLocked() error {
	snap := &snapshot{
		NextSeq: q.nextSeq,
		Jobs:    make([]Job, 0, len(q.order)),
		Groups:  make(map[string]map[string]deliveryState, len(q.groups)),
		Dedup:   q.dedup.snapshotEntries(),
	}
	for _, id := range q.order {
		snap.Jobs = append(snap.Jobs, *q.jobs[id])
	}
	for g, gs := range q.groups {
		states := make(map[string]deliveryState, len(gs))
		for id, st := range gs {
			states[id] = *st
		}
		snap.Groups[g] = states
	}
	return q.st.save(snap)
}
