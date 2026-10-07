package queue

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"
)

// fakeClock 可手动推进的时钟，用于确定性测试租约与退避。
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func testOptions(c *fakeClock) Options {
	return Options{
		MaxRetries:        2,
		BaseBackoff:       time.Second,
		MaxBackoff:        10 * time.Second,
		VisibilityTimeout: 5 * time.Second,
		DedupCapacity:     16,
		Now:               c.Now,
	}
}

func openTestQueue(t *testing.T, path string, c *fakeClock) *Queue {
	t.Helper()
	q, err := Open(path, testOptions(c))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return q
}

// drain 拉空某消费组并确认，返回确认的任务 ID。
func drain(t *testing.T, q *Queue, group string) []string {
	t.Helper()
	var ids []string
	for {
		d, err := q.Poll(group, "drainer")
		if err != nil {
			t.Fatalf("Poll: %v", err)
		}
		if d == nil {
			return ids
		}
		if err := q.Ack(group, d.Job.ID, d.Token); err != nil {
			t.Fatalf("Ack: %v", err)
		}
		ids = append(ids, d.Job.ID)
	}
}

func TestAtLeastOnceDelivery(t *testing.T) {
	dir := t.TempDir()
	c := newFakeClock()

	t.Run("重复入队同一幂等键只入队一次", func(t *testing.T) {
		q := openTestQueue(t, filepath.Join(dir, "dedup.db"), c)
		defer q.Close()
		id1, dup1, _ := q.Enqueue([]byte("a"), "key-1")
		id2, dup2, _ := q.Enqueue([]byte("a"), "key-1")
		id3, dup3, _ := q.Enqueue([]byte("b"), "key-2")
		t.Logf("首次入队 id=%s dup=%v；同键再入队 id=%s dup=%v；异键入队 id=%s dup=%v", id1, dup1, id2, dup2, id3, dup3)
		if dup1 || !dup2 || dup3 {
			t.Fatalf("去重结果不符: dup1=%v dup2=%v dup3=%v", dup1, dup2, dup3)
		}
		if id1 != id2 || id1 == id3 {
			t.Fatalf("任务ID不符: %s %s %s", id1, id2, id3)
		}
		if got := drain(t, q, "g"); len(got) != 2 {
			t.Fatalf("期望2个任务，实际 %v", got)
		}
		t.Logf("结果：通过，队列中只有 2 个任务")
	})

	t.Run("租约超时未确认的任务被重新投递且旧令牌失效", func(t *testing.T) {
		q := openTestQueue(t, filepath.Join(dir, "lease.db"), c)
		defer q.Close()
		id, _, _ := q.Enqueue([]byte("x"), "")
		d1, _ := q.Poll("g", "c1")
		t.Logf("消费者c1拿到任务 %s 第%d次投递，不确认", d1.Job.ID, d1.Attempt)
		if d, _ := q.Poll("g", "c2"); d != nil {
			t.Fatalf("租约未到期，不应投递给其他消费者")
		}
		c.Advance(6 * time.Second) // 超过 VisibilityTimeout
		d2, _ := q.Poll("g", "c2")
		if d2 == nil || d2.Job.ID != id || d2.Attempt != 2 {
			t.Fatalf("租约到期后应重新投递同一任务，got %+v", d2)
		}
		t.Logf("租约到期后消费者c2拿到同一任务，第%d次投递", d2.Attempt)
		if err := q.Ack("g", id, d1.Token); !errors.Is(err, ErrStaleToken) {
			t.Fatalf("旧令牌确认应返回 ErrStaleToken，got %v", err)
		}
		if err := q.Ack("g", id, d2.Token); err != nil {
			t.Fatalf("新令牌确认失败: %v", err)
		}
		t.Logf("结果：通过，旧令牌被拒绝(ErrStaleToken)，新令牌确认成功")
	})

	t.Run("已确认的任务不再投递", func(t *testing.T) {
		q := openTestQueue(t, filepath.Join(dir, "ack.db"), c)
		defer q.Close()
		q.Enqueue([]byte("x"), "")
		d, _ := q.Poll("g", "c1")
		if err := q.Ack("g", d.Job.ID, d.Token); err != nil {
			t.Fatal(err)
		}
		c.Advance(time.Minute)
		if d2, _ := q.Poll("g", "c1"); d2 != nil {
			t.Fatalf("已确认任务不应再次投递，got %+v", d2)
		}
		t.Logf("结果：通过，确认后即使租约超时也不会再投递")
	})
}

func TestConsumerGroups(t *testing.T) {
	dir := t.TempDir()
	c := newFakeClock()

	t.Run("同组并发消费一个任务只落到一个消费者", func(t *testing.T) {
		q := openTestQueue(t, filepath.Join(dir, "conc.db"), c)
		defer q.Close()
		const n = 50
		for i := 0; i < n; i++ {
			q.Enqueue([]byte(fmt.Sprintf("job-%d", i)), "")
		}
		var mu sync.Mutex
		got := map[string]int{} // jobID -> 被投递次数
		var wg sync.WaitGroup
		for w := 0; w < 8; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				consumer := fmt.Sprintf("c%d", w)
				for {
					d, err := q.Poll("g", consumer)
					if err != nil {
						t.Errorf("Poll: %v", err)
						return
					}
					if d == nil {
						return
					}
					mu.Lock()
					got[d.Job.ID]++
					mu.Unlock()
					if err := q.Ack("g", d.Job.ID, d.Token); err != nil {
						t.Errorf("Ack: %v", err)
						return
					}
				}
			}(w)
		}
		wg.Wait()
		if len(got) != n {
			t.Fatalf("期望 %d 个任务被消费，实际 %d", n, len(got))
		}
		for id, cnt := range got {
			if cnt != 1 {
				t.Fatalf("任务 %s 被投递 %d 次，期望恰好 1 次", id, cnt)
			}
		}
		t.Logf("结果：通过，8 个消费者并发消费 %d 个任务，每个任务恰好被一个消费者拿到", n)
	})

	t.Run("不同消费组各自拿到全量任务", func(t *testing.T) {
		q := openTestQueue(t, filepath.Join(dir, "groups.db"), c)
		defer q.Close()
		for i := 0; i < 5; i++ {
			q.Enqueue([]byte(fmt.Sprintf("job-%d", i)), "")
		}
		a := drain(t, q, "group-A")
		b := drain(t, q, "group-B")
		sort.Strings(a)
		sort.Strings(b)
		if len(a) != 5 || len(b) != 5 {
			t.Fatalf("两组都应拿到5个任务: A=%v B=%v", a, b)
		}
		for i := range a {
			if a[i] != b[i] {
				t.Fatalf("两组任务不一致: A=%v B=%v", a, b)
			}
		}
		t.Logf("结果：通过，group-A 与 group-B 各自独立拿到全部 5 个任务")
	})
}

func TestRetryAndDeadLetter(t *testing.T) {
	dir := t.TempDir()
	c := newFakeClock()

	t.Run("可重试失败按退避重投且超过上限进死信", func(t *testing.T) {
		q := openTestQueue(t, filepath.Join(dir, "retry.db"), c)
		defer q.Close()
		id, _, _ := q.Enqueue([]byte("x"), "")

		// 第 1 次投递失败 -> 退避 1s
		d, _ := q.Poll("g", "c1")
		q.Nack("g", id, d.Token, FailureRetryable, errors.New("临时错误"))
		if d2, _ := q.Poll("g", "c1"); d2 != nil {
			t.Fatalf("退避期间不应投递")
		}
		t.Logf("第1次失败，退避1s内不可投递")
		c.Advance(time.Second)
		d, _ = q.Poll("g", "c1")
		if d == nil || d.Attempt != 2 {
			t.Fatalf("退避后应第2次投递，got %+v", d)
		}

		// 第 2 次投递失败 -> 退避 2s
		q.Nack("g", id, d.Token, FailureRetryable, errors.New("临时错误"))
		c.Advance(time.Second)
		if d2, _ := q.Poll("g", "c1"); d2 != nil {
			t.Fatalf("退避2s未到期，不应投递")
		}
		c.Advance(time.Second)
		d, _ = q.Poll("g", "c1")
		if d == nil || d.Attempt != 3 {
			t.Fatalf("应第3次投递，got %+v", d)
		}
		t.Logf("第2次失败退避2s后第3次投递")

		// 第 3 次失败 -> 超过 MaxRetries=2，进死信
		q.Nack("g", id, d.Token, FailureRetryable, errors.New("临时错误"))
		c.Advance(time.Minute)
		if d2, _ := q.Poll("g", "c1"); d2 != nil {
			t.Fatalf("进死信后不应再投递")
		}
		dls := q.DeadLetters("g")
		if len(dls) != 1 || dls[0].Job.ID != id || dls[0].Attempts != 3 {
			t.Fatalf("死信记录不符: %+v", dls)
		}
		t.Logf("结果：通过，重试2次后超过上限进死信: attempts=%d reason=%q", dls[0].Attempts, dls[0].Reason)
	})

	t.Run("不可重试失败直接进死信且可单独查询", func(t *testing.T) {
		q := openTestQueue(t, filepath.Join(dir, "fatal.db"), c)
		defer q.Close()
		id, _, _ := q.Enqueue([]byte("bad"), "")
		d, _ := q.Poll("g", "c1")
		q.Nack("g", id, d.Token, FailureFatal, errors.New("数据格式非法"))
		dls := q.DeadLetters("g")
		if len(dls) != 1 || dls[0].Attempts != 1 {
			t.Fatalf("fatal 失败应直接进死信: %+v", dls)
		}
		if d2, _ := q.Poll("g", "c1"); d2 != nil {
			t.Fatalf("死信任务不应再投递")
		}
		t.Logf("结果：通过，fatal 失败第1次即进死信: reason=%q", dls[0].Reason)
	})
}

func TestRestartRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "restart.db")
	c := newFakeClock()

	t.Run("重启后未确认任务重新投递且状态不丢不错位", func(t *testing.T) {
		q := openTestQueue(t, path, c)
		// a: 确认; b: 重试中(1次失败); c: 投递中未确认; d: 未投递; dead: 死信
		q.Enqueue([]byte("a"), "k-a")
		q.Enqueue([]byte("b"), "k-b")
		q.Enqueue([]byte("c"), "k-c")
		q.Enqueue([]byte("d"), "k-d")
		q.Enqueue([]byte("dead"), "k-dead")

		d, _ := q.Poll("g", "c1") // a
		q.Ack("g", d.Job.ID, d.Token)
		d, _ = q.Poll("g", "c1") // b
		q.Nack("g", d.Job.ID, d.Token, FailureRetryable, errors.New("临时错误"))
		c.Advance(2 * time.Second) // 让 b 的退避到期
		d, _ = q.Poll("g", "c1") // b 第2次
		bID, bToken := d.Job.ID, d.Token
		q.Nack("g", bID, bToken, FailureRetryable, errors.New("临时错误")) // b 重试中，attempts=2
		d, _ = q.Poll("g", "c1") // c 投递中，不确认
		cID := d.Job.ID
		d, _ = q.Poll("g", "c1") // d
		dID := d.Job.ID
		q.Nack("g", dID, d.Token, FailureFatal, errors.New("致命错误")) // d 死信
		if err := q.Close(); err != nil {
			t.Fatal(err)
		}
		t.Logf("关闭前状态: a=acked b=retrying(attempts=2) c=inflight d=dead")

		// 模拟进程重启
		c.Advance(2 * time.Second) // b 的退避(2s)到期
		q2 := openTestQueue(t, path, c)
		defer q2.Close()

		// 死信恢复
		dls := q2.DeadLetters("g")
		if len(dls) != 1 || dls[0].Job.ID != dID {
			t.Fatalf("重启后死信应恢复: %+v", dls)
		}
		// 幂等去重恢复：重复入队已确认的 a 仍被去重
		if id, dup, _ := q2.Enqueue([]byte("a"), "k-a"); !dup || id == "" {
			t.Fatalf("重启后去重记录应恢复: id=%s dup=%v", id, dup)
		}
		// 拉取：应依次拿到 b(第3次投递，重试次数恢复) 和 c(重启重新投递)，d 不再出现
		got := map[string]int{}
		for i := 0; i < 4; i++ {
			d, _ := q2.Poll("g", "c2")
			if d == nil {
				break
			}
			got[d.Job.ID] = d.Attempt
			q2.Ack("g", d.Job.ID, d.Token)
		}
		t.Logf("重启后投递情况: %v", got)
		if got[bID] != 3 {
			t.Fatalf("b 的重试次数应恢复为第3次投递，got %d", got[bID])
		}
		if got[cID] != 2 {
			// c 重启前已投递过 1 次未确认，重启后重新投递应为第 2 次
			t.Fatalf("c 应被重新投递为第2次，got attempt=%d", got[cID])
		}
		if _, ok := got[dID]; ok {
			t.Fatalf("死信任务 d 不应再投递")
		}
		if len(got) != 3 { // b, c, 以及未投递过的 d? 不，d 是死信；还有 "d" 任务已死信
			t.Fatalf("重启后应只有 b/c 及未投递任务被投递，got %v", got)
		}
		t.Logf("结果：通过，重启后 acked 不重投、inflight 重投、重试次数与死信完整恢复")
	})
}

func TestDedupCapacity(t *testing.T) {
	dir := t.TempDir()
	c := newFakeClock()

	t.Run("容量满时淘汰已终结任务且不淘汰重试中的任务", func(t *testing.T) {
		opts := testOptions(c)
		opts.DedupCapacity = 4
		q, err := Open(filepath.Join(dir, "cap.db"), opts)
		if err != nil {
			t.Fatal(err)
		}
		defer q.Close()
		// 占满容量：k1 确认终结，k2 重试中，k3/k4 未投递
		q.Enqueue([]byte("1"), "k1")
		q.Enqueue([]byte("2"), "k2")
		q.Enqueue([]byte("3"), "k3")
		q.Enqueue([]byte("4"), "k4")
		d, _ := q.Poll("g", "c1") // k1
		k1 := d.Job.ID
		q.Ack("g", k1, d.Token)
		d, _ = q.Poll("g", "c1") // k2
		k2 := d.Job.ID
		q.Nack("g", k2, d.Token, FailureRetryable, errors.New("临时错误")) // k2 重试中

		// 容量满，k1 已终结可淘汰，k2 重试中不可淘汰
		id5, _, err := q.Enqueue([]byte("5"), "k5")
		if err != nil {
			t.Fatalf("应淘汰已终结的 k1 后成功入队: %v", err)
		}
		t.Logf("k5 入队成功(id=%s)，被淘汰的是已确认的 k1", id5)
		if _, dup, _ := q.Enqueue([]byte("1"), "k1"); dup {
			t.Fatalf("k1 的记录已被淘汰，再入队不应判重")
		}
		if _, dup, _ := q.Enqueue([]byte("2"), "k2"); !dup {
			t.Fatalf("k2 仍在重试中，其去重记录不得被淘汰")
		}
		t.Logf("结果：通过，重试中的 k2 去重记录保留，已终结的 k1 被淘汰")
	})

	t.Run("全部记录对应任务都在处理中时拒绝入队", func(t *testing.T) {
		opts := testOptions(c)
		opts.DedupCapacity = 4
		q, err := Open(filepath.Join(dir, "full.db"), opts)
		if err != nil {
			t.Fatal(err)
		}
		defer q.Close()
		for i := 0; i < 4; i++ {
			q.Enqueue([]byte(fmt.Sprintf("%d", i)), fmt.Sprintf("kf%d", i))
		}
		// 不消费，所有任务都在处理中，无可淘汰记录
		_, _, err = q.Enqueue([]byte("x"), "kf-x")
		if !errors.Is(err, ErrDedupFull) {
			t.Fatalf("无可淘汰记录时应返回 ErrDedupFull，got %v", err)
		}
		t.Logf("结果：通过，去重表满且无可淘汰记录时返回 ErrDedupFull")
	})
}
