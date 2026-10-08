package queue

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func testOptions(dir string) Options {
	return Options{
		Dir:           dir,
		DedupCapacity: 8,
		Lease:         60 * time.Millisecond,
		MaxAttempts:   3,
		BackoffBase:   40 * time.Millisecond,
		BackoffMax:    200 * time.Millisecond,
	}
}

func TestDuplicateDeliveryAndAck(t *testing.T) {
	t.Log("业务: 投递与确认(至少一次) / 用例: 未确认任务租约超时后重复投递, 确认后不再投递, 重复确认幂等")
	q, err := Open(testOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	id, inserted, err := q.Enqueue(Job{DedupKey: "k1", Payload: []byte("hello")})
	if err != nil || !inserted {
		t.Fatalf("enqueue: id=%s inserted=%v err=%v", id, inserted, err)
	}

	d1, err := q.Pull("g1")
	if err != nil || d1 == nil {
		t.Fatalf("first pull: %+v err=%v", d1, err)
	}
	t.Logf("  第一次投递 job=%s attempt=%d", d1.Job.ID, d1.Attempts)

	// 不确认，租约内不能再被投出。
	if d, _ := q.Pull("g1"); d != nil {
		t.Fatalf("租约内不应重复投递, got %+v", d)
	}
	// 租约超时后必须重新投递（至少一次）。
	time.Sleep(70 * time.Millisecond)
	d2, err := q.Pull("g1")
	if err != nil || d2 == nil || d2.Job.ID != id {
		t.Fatalf("租约超时后应重投同一任务, got %+v err=%v", d2, err)
	}
	t.Logf("  租约超时重投 job=%s attempt=%d", d2.Job.ID, d2.Attempts)
	if d2.Attempts != 2 {
		t.Fatalf("重投次数应为 2, got %d", d2.Attempts)
	}

	// 确认 + 重复确认（幂等）。
	if err := q.Ack("g1", id); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if err := q.Ack("g1", id); err != nil {
		t.Fatalf("重复 ack 应幂等成功: %v", err)
	}
	t.Log("  确认成功, 重复确认幂等成功")

	// 已确认任务不得再次投递。
	time.Sleep(70 * time.Millisecond)
	if d, _ := q.Pull("g1"); d != nil {
		t.Fatalf("已确认任务不得再次投递, got %+v", d)
	}
	t.Log("  确认后不再投递 OK")
}

func TestIdempotentDedup(t *testing.T) {
	t.Log("业务: 幂等去重 / 用例: 相同 DedupKey 重复入队返回同一任务, 队列中只有一条")
	q, err := Open(testOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	id1, ins1, _ := q.Enqueue(Job{DedupKey: "dup", Payload: []byte("v1")})
	id2, ins2, _ := q.Enqueue(Job{DedupKey: "dup", Payload: []byte("v2")})
	if !ins1 || ins2 || id1 != id2 {
		t.Fatalf("去重失败: id1=%s ins1=%v id2=%s ins2=%v", id1, ins1, id2, ins2)
	}
	t.Logf("  重复入队被去重, 返回已有 job=%s", id1)

	d, _ := q.Pull("g")
	if d == nil || string(d.Job.Payload) != "v1" {
		t.Fatalf("应只保留首次入队的任务, got %+v", d)
	}
	if d2, _ := q.Pull("g"); d2 != nil {
		t.Fatalf("队列中不应有第二条任务, got %+v", d2)
	}
	t.Log("  队列中只有一条任务, 载荷为首次入队内容 OK")
}

func TestConcurrentConsumersSameGroup(t *testing.T) {
	t.Log("业务: 消费组并发消费 / 用例: 同组多个消费者并行拉取, 一个任务不会同时落到两个消费者手里")
	q, err := Open(testOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	const n = 50
	for i := 0; i < n; i++ {
		if _, _, err := q.Enqueue(Job{DedupKey: fmt.Sprintf("c-%d", i)}); err != nil {
			t.Fatal(err)
		}
	}

	var mu sync.Mutex
	held := map[string]int{} // jobID -> 同时持有该任务的消费者数（校验用）
	done := map[string]int{} // jobID -> 被确认次数
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				d, err := q.Pull("workers")
				if err != nil {
					t.Errorf("pull: %v", err)
					return
				}
				if d == nil {
					return
				}
				mu.Lock()
				held[d.Job.ID]++
				if held[d.Job.ID] > 1 {
					t.Errorf("任务 %s 同时落到多个消费者手里", d.Job.ID)
				}
				mu.Unlock()
				time.Sleep(time.Millisecond) // 模拟处理
				if err := q.Ack("workers", d.Job.ID); err != nil {
					t.Errorf("ack: %v", err)
				}
				mu.Lock()
				held[d.Job.ID]--
				done[d.Job.ID]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(done) != n {
		t.Fatalf("应有 %d 个任务被确认, got %d", n, len(done))
	}
	for id, c := range done {
		if c != 1 {
			t.Fatalf("任务 %s 被确认 %d 次, 期望恰好 1 次", id, c)
		}
	}
	t.Logf("  %d 个任务各被恰好一个消费者处理并确认 OK", n)
}

func TestDifferentGroupsGetAllJobs(t *testing.T) {
	t.Log("业务: 消费组 / 用例: 不同分组各自拿到全量任务")
	q, err := Open(testOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	const n = 5
	for i := 0; i < n; i++ {
		q.Enqueue(Job{DedupKey: fmt.Sprintf("fan-%d", i)})
	}
	for _, g := range []string{"groupA", "groupB"} {
		got := 0
		for {
			d, _ := q.Pull(g)
			if d == nil {
				break
			}
			got++
			if err := q.Ack(g, d.Job.ID); err != nil {
				t.Fatal(err)
			}
		}
		if got != n {
			t.Fatalf("组 %s 应拿到 %d 个任务, got %d", g, n, got)
		}
		t.Logf("  组 %s 拿到全部 %d 个任务 OK", g, n)
	}
}

func TestNackUnknownJob(t *testing.T) {
	t.Log("业务: 投递与确认 / 用例: 对未知任务或未投递任务 Ack/Nack 返回可区分错误")
	q, err := Open(testOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if err := q.Ack("g", "nope"); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("期望 ErrJobNotFound, got %v", err)
	}
	id, _, _ := q.Enqueue(Job{DedupKey: "x"})
	if err := q.Nack("g", id, FailRetryable, nil); !errors.Is(err, ErrNotInflight) {
		t.Fatalf("期望 ErrNotInflight, got %v", err)
	}
	t.Log("  未知任务与未投递任务的错误均可区分 OK")
}
