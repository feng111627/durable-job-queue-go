package queue

import (
	"fmt"
	"sync"
	"time"
)

// Queue 是单机持久化任务队列，支持多消费组并发消费。
//
// 投递语义：至少一次（at-least-once）。任务被确认前可能因租约超时、
// 失败重试或进程重启被重复投递；是否“重复执行”由消费方幂等保证，
// 队列保证：已确认的任务不再投递，未确认的任务绝不丢失。
//
// 并发安全：所有方法可并发调用，内部由互斥锁串行化；
// 同组内一个任务同一时刻只会投递给一个消费者（租约 + 令牌保证）。
type Queue struct {
	mu       sync.Mutex
	opts     Options
	store    *store
	jobs     map[string]*Job
	order    []string // 入队顺序的任务 ID，任务一旦入队不删除
	groups   map[string]map[string]*deliveryRecord
	dedup    *dedup
	tokenSeq uint64
	closed   bool
}

// Open 打开（或创建）位于 path 的队列，并恢复重启前状态：
// 重启前处于投递中（inflight）但未确认的任务一律回到待投递状态，
// 等待重新投递；已确认与已死信的状态原样恢复。
func Open(path string, opts Options) (*Queue, error) {
	o := opts.withDefaults()
	st := newStore(path)
	loaded, err := st.load()
	if err != nil {
		return nil, err
	}
	q := &Queue{
		opts:   o,
		store:  st,
		jobs:   map[string]*Job{},
		groups: loaded.Groups,
		dedup:  newDedup(o.DedupCapacity, loaded.Dedup),
	}
	for _, j := range loaded.Jobs {
		job := *j
		q.jobs[job.ID] = &job
		q.order = append(q.order, job.ID)
	}
	// 恢复：崩溃/重启时持有租约的任务视为未确认，回到待投递。
	now := o.Now()
	for _, recs := range q.groups {
		for _, rec := range recs {
			if rec.State == stateInflight {
				rec.State = statePending
				rec.Consumer = ""
				rec.Token = ""
				rec.VisibleAt = now
			}
		}
	}
	if err := q.persistLocked(); err != nil {
		return nil, err
	}
	return q, nil
}

// Close 关闭队列并落盘最终状态。
func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil
	}
	q.closed = true
	return q.persistLocked()
}

// Enqueue 入队任务。key 非空时做幂等去重；
// 返回 dup=true 表示该 key 已存在，未重复入队，jobID 为已存在任务的 ID。
func (q *Queue) Enqueue(payload []byte, key string) (jobID string, dup bool, err error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return "", false, ErrClosed
	}
	if key != "" {
		if id, ok := q.dedup.lookup(key); ok {
			return id, true, nil
		}
	}
	id := fmt.Sprintf("j%06d", len(q.order)+1)
	if key != "" {
		if err := q.dedup.add(key, id, q.jobFinishedLocked); err != nil {
			return "", false, err
		}
	}
	job := &Job{ID: id, Key: key, Payload: payload, EnqueuedAt: q.opts.Now()}
	q.jobs[id] = job
	q.order = append(q.order, id)
	return id, false, q.persistLocked()
}

// Poll 为消费组 group 中的消费者 consumer 拉取一个可投递任务；
// 没有可投递任务时返回 (nil, nil)。
// 同一任务在同组内同一时刻只会被一个消费者持有（租约 + 令牌）。
func (q *Queue) Poll(group, consumer string) (*Delivery, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, ErrClosed
	}
	now := q.opts.Now()
	recs := q.groupRecordsLocked(group)
	// 租约到期未确认的任务回到待投递，等待重新投递。
	for _, rec := range recs {
		if rec.State == stateInflight && !rec.VisibleAt.After(now) {
			rec.State = statePending
			rec.Consumer = ""
			rec.Token = ""
		}
	}
	for _, id := range q.order {
		rec := recs[id]
		if rec == nil {
			rec = &deliveryRecord{State: statePending}
			recs[id] = rec
		}
		if rec.State != statePending || rec.VisibleAt.After(now) {
			continue
		}
		rec.State = stateInflight
		rec.Attempts++
		rec.Consumer = consumer
		q.tokenSeq++
		rec.Token = fmt.Sprintf("t%d", q.tokenSeq)
		rec.VisibleAt = now.Add(q.opts.VisibilityTimeout)
		if err := q.persistLocked(); err != nil {
			return nil, err
		}
		job := *q.jobs[id]
		return &Delivery{Job: job, Group: group, Token: rec.Token, Attempt: rec.Attempts}, nil
	}
	return nil, q.persistLocked()
}

// Ack 确认任务已处理完成；已确认的任务不再投递。
func (q *Queue) Ack(group, jobID, token string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrClosed
	}
	rec, err := q.inflightLocked(group, jobID, token)
	if err != nil {
		return err
	}
	rec.State = stateAcked
	rec.Consumer = ""
	rec.Token = ""
	return q.persistLocked()
}

// Nack 报告任务处理失败；kind 决定进入重试（带退避与次数上限）还是直接死信。
func (q *Queue) Nack(group, jobID, token string, kind FailureKind, cause error) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrClosed
	}
	rec, err := q.inflightLocked(group, jobID, token)
	if err != nil {
		return err
	}
	now := q.opts.Now()
	rec.Consumer = ""
	rec.Token = ""
	rec.LastError = fmt.Sprintf("%s: %v", kind, cause)
	if kind == FailureFatal || rec.Attempts > q.opts.MaxRetries {
		// 不可重试，或重试次数已超上限：进死信。
		rec.State = stateDead
		rec.VisibleAt = now // 记录进入死信的时间
		return q.persistLocked()
	}
	// 可重试：按指数退避安排下次投递时间。
	rec.State = statePending
	rec.VisibleAt = now.Add(q.backoff(rec.Attempts))
	return q.persistLocked()
}

// DeadLetters 返回该消费组的死信列表（按入队顺序）。
func (q *Queue) DeadLetters(group string) []DeadLetter {
	q.mu.Lock()
	defer q.mu.Unlock()
	var out []DeadLetter
	recs := q.groups[group]
	for _, id := range q.order {
		rec := recs[id]
		if rec == nil || rec.State != stateDead {
			continue
		}
		out = append(out, DeadLetter{
			Job:      *q.jobs[id],
			Group:    group,
			Attempts: rec.Attempts,
			Reason:   rec.LastError,
			FailedAt: rec.VisibleAt,
		})
	}
	return out
}

// backoff 计算第 n 次投递失败后的退避时长：BaseBackoff * 2^(n-1)，封顶 MaxBackoff。
func (q *Queue) backoff(attempts int) time.Duration {
	d := q.opts.BaseBackoff
	for i := 1; i < attempts; i++ {
		d *= 2
		if d >= q.opts.MaxBackoff {
			return q.opts.MaxBackoff
		}
	}
	return d
}

// groupRecordsLocked 返回（必要时创建）消费组的投递记录表。
func (q *Queue) groupRecordsLocked(group string) map[string]*deliveryRecord {
	recs := q.groups[group]
	if recs == nil {
		recs = map[string]*deliveryRecord{}
		q.groups[group] = recs
	}
	return recs
}

// inflightLocked 校验任务在该组处于投递中且令牌匹配。
func (q *Queue) inflightLocked(group, jobID, token string) (*deliveryRecord, error) {
	rec := q.groups[group][jobID]
	if rec == nil || rec.State != stateInflight {
		return nil, ErrUnknownJob
	}
	if rec.Token != token {
		return nil, ErrStaleToken
	}
	return rec, nil
}

// jobFinishedLocked 判断任务是否可被淘汰出去重表：
// 至少被一个消费组投递过，且在所有已知消费组中都已终结（确认或死信）。
// 仍在等待投递或重试中的任务一律返回 false，防止误淘汰。
func (q *Queue) jobFinishedLocked(jobID string) bool {
	seen := false
	for _, recs := range q.groups {
		rec := recs[jobID]
		if rec == nil {
			continue
		}
		seen = true
		if rec.State != stateAcked && rec.State != stateDead {
			return false
		}
	}
	return seen
}

// persistLocked 将当前状态整体快照落盘。调用时必须持有锁。
func (q *Queue) persistLocked() error {
	st := &diskState{Groups: q.groups, Dedup: q.dedup.st}
	for _, id := range q.order {
		st.Jobs = append(st.Jobs, q.jobs[id])
	}
	return q.store.save(st)
}
