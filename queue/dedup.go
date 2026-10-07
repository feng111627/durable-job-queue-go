package queue

import "sort"

// dedupEntry 是一条幂等去重记录。
type dedupEntry struct {
	JobID string `json:"job_id"`
	Seq   uint64 `json:"seq"` // 单调递增的插入序号，用于确定淘汰顺序
}

// dedupState 是去重表的可持久化状态。
type dedupState struct {
	Entries map[string]dedupEntry `json:"entries"` // key -> entry
	NextSeq uint64                `json:"next_seq"`
}

func newDedupState() *dedupState {
	return &dedupState{Entries: map[string]dedupEntry{}}
}

// dedup 提供容量受限的幂等去重。
//
// 淘汰策略（确定且可解释）：容量满时，按插入序号从旧到新扫描，
// 淘汰第一条“对应任务已在所有消费组终结（全部确认或全部死信）”的记录；
// 仍在投递/重试中的任务记录不得淘汰；若没有任何可淘汰记录，拒绝写入并
// 返回 ErrDedupFull，由调用方决定稍后重试或放弃去重。
type dedup struct {
	capacity int
	st       *dedupState
}

func newDedup(capacity int, st *dedupState) *dedup {
	if st == nil {
		st = newDedupState()
	}
	return &dedup{capacity: capacity, st: st}
}

// lookup 查询幂等键是否已存在，存在则返回对应任务 ID。
func (d *dedup) lookup(key string) (string, bool) {
	e, ok := d.st.Entries[key]
	return e.JobID, ok
}

// add 记录幂等键；超出容量时按上述策略淘汰，无可淘汰记录时返回 ErrDedupFull。
// canEvict(jobID) 由队列提供，判断任务是否已在所有消费组终结。
func (d *dedup) add(key, jobID string, canEvict func(jobID string) bool) error {
	if _, ok := d.st.Entries[key]; ok {
		return nil // 已存在，幂等
	}
	if len(d.st.Entries) >= d.capacity {
		if !d.evictOne(canEvict) {
			return ErrDedupFull
		}
	}
	d.st.NextSeq++
	d.st.Entries[key] = dedupEntry{JobID: jobID, Seq: d.st.NextSeq}
	return nil
}

// evictOne 按插入顺序淘汰第一条可淘汰记录，返回是否成功。
func (d *dedup) evictOne(canEvict func(jobID string) bool) bool {
	keys := make([]string, 0, len(d.st.Entries))
	for k := range d.st.Entries {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return d.st.Entries[keys[i]].Seq < d.st.Entries[keys[j]].Seq
	})
	for _, k := range keys {
		if canEvict(d.st.Entries[k].JobID) {
			delete(d.st.Entries, k)
			return true
		}
	}
	return false
}
