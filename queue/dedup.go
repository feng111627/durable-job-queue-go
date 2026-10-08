package queue

import "container/list"

// dedupEntry 是一条去重记录：dedupKey -> jobID。
type dedupEntry struct {
	Key   string `json:"key"`
	JobID string `json:"job_id"`
}

// dedupIndex 是幂等去重索引，容量受限。
//
// 淘汰策略（确定且可解释）：
//   - 命中或新增都会把键移到“最新”一端；
//   - 容量满时，从“最久未使用”一端开始找第一个允许淘汰的键淘汰之；
//   - 仍在投递中/重试中的任务对应的键由 evictable 回调判定为不可淘汰，
//     保证重试中的任务不会被误淘汰；
//   - 若所有键都不可淘汰，返回 ErrDedupFull，由调用方拒绝入队。
type dedupIndex struct {
	capacity int
	ll       *list.List               // 前端=最新，后端=最久未使用；元素为 dedupEntry
	items    map[string]*list.Element // key -> 链表节点
}

func newDedupIndex(capacity int) *dedupIndex {
	return &dedupIndex{
		capacity: capacity,
		ll:       list.New(),
		items:    make(map[string]*list.Element),
	}
}

// lookup 返回键对应的 jobID 及是否存在；命中会刷新其热度。
func (d *dedupIndex) lookup(key string) (string, bool) {
	el, ok := d.items[key]
	if !ok {
		return "", false
	}
	d.ll.MoveToFront(el)
	return el.Value.(dedupEntry).JobID, true
}

// peek 返回键对应的 jobID 及是否存在，不刷新热度。
// 供淘汰判定等只读场景使用，避免在淘汰扫描中改变链表顺序。
func (d *dedupIndex) peek(key string) (string, bool) {
	el, ok := d.items[key]
	if !ok {
		return "", false
	}
	return el.Value.(dedupEntry).JobID, true
}

// add 插入键并置于最新端；超出容量时按上述策略淘汰。
// evictable 返回 false 表示该键不可淘汰（任务仍在投递/重试中）。
func (d *dedupIndex) add(key, jobID string, evictable func(key string) bool) error {
	if el, ok := d.items[key]; ok {
		el.Value = dedupEntry{Key: key, JobID: jobID}
		d.ll.MoveToFront(el)
		return nil
	}
	for d.ll.Len() >= d.capacity {
		if !d.evictOne(evictable) {
			return ErrDedupFull
		}
	}
	el := d.ll.PushFront(dedupEntry{Key: key, JobID: jobID})
	d.items[key] = el
	return nil
}

// evictOne 从最久未使用端找第一个可淘汰的键淘汰；没有则返回 false。
func (d *dedupIndex) evictOne(evictable func(key string) bool) bool {
	for el := d.ll.Back(); el != nil; el = el.Prev() {
		e := el.Value.(dedupEntry)
		if evictable == nil || evictable(e.Key) {
			delete(d.items, e.Key)
			d.ll.Remove(el)
			return true
		}
	}
	return false
}

// remove 删除键。
func (d *dedupIndex) remove(key string) {
	if el, ok := d.items[key]; ok {
		delete(d.items, key)
		d.ll.Remove(el)
	}
}

// len 返回当前键数量。
func (d *dedupIndex) len() int { return d.ll.Len() }

// snapshotEntries 按“最新->最旧”的顺序导出全部记录，保证恢复后淘汰顺序一致。
func (d *dedupIndex) snapshotEntries() []dedupEntry {
	out := make([]dedupEntry, 0, d.ll.Len())
	for el := d.ll.Front(); el != nil; el = el.Next() {
		out = append(out, el.Value.(dedupEntry))
	}
	return out
}

// restore 从有序快照恢复（输入为“最新->最旧”），不触发淘汰。
func (d *dedupIndex) restore(entries []dedupEntry) {
	d.ll.Init()
	d.items = make(map[string]*list.Element, len(entries))
	for _, e := range entries {
		el := d.ll.PushBack(e) // 依次追加，保持 front=最新
		d.items[e.Key] = el
	}
}
