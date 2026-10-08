package queue

import (
	"errors"
	"testing"
	"time"
)

func TestRetryWithBackoffAndDeadLetter(t *testing.T) {
	t.Log("业务: 失败重试与死信 / 用例: 可重试失败按退避重投, 超过次数上限进死信并可单独查询")
	q, err := Open(testOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	id, _, _ := q.Enqueue(Job{DedupKey: "flaky", MaxAttempts: 3})
	cause := errors.New("boom")

	// 第 1 次投递后失败 -> 进入退避，退避窗口内不可拉取。
	d, _ := q.Pull("g")
	if err := q.Nack("g", id, FailRetryable, cause); err != nil {
		t.Fatal(err)
	}
	t.Logf("  第 %d 次投递失败(retryable), 进入退避", d.Attempts)
	if d2, _ := q.Pull("g"); d2 != nil {
		t.Fatalf("退避窗口内不应重投, got %+v", d2)
	}
	t.Log("  退避窗口内不重投 OK")

	// 退避到期后重投，直到达到上限进死信。
	for want := 2; want <= 3; want++ {
		time.Sleep(250 * time.Millisecond) // 超过 BackoffMax
		d, err := q.Pull("g")
		if err != nil || d == nil {
			t.Fatalf("退避到期后应重投: d=%+v err=%v", d, err)
		}
		if d.Attempts != want {
			t.Fatalf("第 %d 次重投, got attempt=%d", want, d.Attempts)
		}
		t.Logf("  退避到期重投 attempt=%d", d.Attempts)
		if err := q.Nack("g", id, FailRetryable, cause); err != nil {
			t.Fatal(err)
		}
	}

	// 超过上限：进死信，不再投递。
	time.Sleep(250 * time.Millisecond)
	if d, _ := q.Pull("g"); d != nil {
		t.Fatalf("死信任务不得再投递, got %+v", d)
	}
	dead := q.DeadLetters("g")
	if len(dead) != 1 || dead[0].Job.ID != id {
		t.Fatalf("应有一条死信, got %+v", dead)
	}
	if dead[0].Attempts != 3 || dead[0].LastFail != FailRetryable || dead[0].LastError != "boom" {
		t.Fatalf("死信信息不完整: %+v", dead[0])
	}
	t.Logf("  超过上限进入死信 attempts=%d lastFail=%s err=%q OK",
		dead[0].Attempts, dead[0].LastFail, dead[0].LastError)
}

func TestFatalGoesStraightToDeadLetter(t *testing.T) {
	t.Log("业务: 失败重试与死信 / 用例: 致命失败不重试, 直接进死信")
	q, err := Open(testOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	id, _, _ := q.Enqueue(Job{DedupKey: "fatal", MaxAttempts: 5})
	q.Pull("g")
	if err := q.Nack("g", id, FailFatal, errors.New("bad payload")); err != nil {
		t.Fatal(err)
	}
	dead := q.DeadLetters("g")
	if len(dead) != 1 || dead[0].LastFail != FailFatal || dead[0].Attempts != 1 {
		t.Fatalf("致命失败应直接进死信: %+v", dead)
	}
	t.Logf("  致命失败直接进死信 attempts=%d lastFail=%s OK", dead[0].Attempts, dead[0].LastFail)
}

func TestRestartRecovery(t *testing.T) {
	t.Log("业务: 重启恢复 / 用例: 重启后未确认任务重新投递, 重试次数与死信保留, 已确认任务不再投递, 不产生重复或错位")
	dir := t.TempDir()
	opts := testOptions(dir)

	q, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	// done: 已确认; retrying: 失败 1 次等待重试; inflightJob: 拉到一半进程“崩溃”; dead: 进死信。
	q.Enqueue(Job{DedupKey: "done"})
	q.Enqueue(Job{DedupKey: "retrying", MaxAttempts: 3})
	q.Enqueue(Job{DedupKey: "inflight"})
	q.Enqueue(Job{DedupKey: "dead", MaxAttempts: 1})

	d, _ := q.Pull("g") // done
	if err := q.Ack("g", d.Job.ID); err != nil {
		t.Fatal(err)
	}
	d, _ = q.Pull("g") // retrying，失败一次
	retryingID := d.Job.ID
	q.Nack("g", retryingID, FailRetryable, errors.New("try again"))
	d, _ = q.Pull("g") // inflight：不确认，模拟崩溃时处理到一半
	inflightID := d.Job.ID
	d, _ = q.Pull("g") // dead：一次失败即死信
	deadID := d.Job.ID
	q.Nack("g", deadID, FailRetryable, errors.New("give up"))
	t.Logf("  崩溃前: done=1 retrying=%s(attempts=1) inflight=%s dead=%s", retryingID, inflightID, deadID)
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启。
	q2, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer q2.Close()

	// 死信保留。
	dead := q2.DeadLetters("g")
	if len(dead) != 1 || dead[0].Job.ID != deadID {
		t.Fatalf("重启后死信应保留, got %+v", dead)
	}
	t.Log("  重启后死信保留 OK")

	// 重试次数保留：retrying 再次投递应为第 2 次。
	time.Sleep(250 * time.Millisecond)
	seen := map[string]int{}
	for {
		d, err := q2.Pull("g")
		if err != nil {
			t.Fatal(err)
		}
		if d == nil {
			break
		}
		seen[d.Job.ID] = d.Attempts
		q2.Ack("g", d.Job.ID)
	}
	if _, ok := seen["job-000001"]; ok {
		t.Fatal("已确认任务重启后不得再次投递")
	}
	if seen[retryingID] != 2 {
		t.Fatalf("重试次数应恢复, %s 第 2 次投递, got %d", retryingID, seen[retryingID])
	}
	if seen[inflightID] != 2 {
		t.Fatalf("崩溃时处理到一半的任务应重新投递, %s attempt 应为 2, got %d", inflightID, seen[inflightID])
	}
	if _, ok := seen[deadID]; ok {
		t.Fatal("死信任务不得再次投递")
	}
	if len(seen) != 2 {
		t.Fatalf("重启后应恰好重投 2 个任务(无重复无错位), got %v", seen)
	}
	t.Logf("  重启后重投: %v, 已确认与死信均未重投 OK", seen)

	// 再次重启：全部已确认/死信，不应有任何投递。
	q2.Close()
	q3, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer q3.Close()
	time.Sleep(250 * time.Millisecond)
	if d, _ := q3.Pull("g"); d != nil {
		t.Fatalf("二次重启后不应再有可投递任务, got %+v", d)
	}
	t.Log("  二次重启后无重复投递 OK")
}

func TestDedupSurvivesRestart(t *testing.T) {
	t.Log("业务: 幂等去重+重启恢复 / 用例: 重启后相同 DedupKey 仍被去重, 任务 ID 不复用")
	dir := t.TempDir()
	opts := testOptions(dir)
	q, _ := Open(opts)
	id1, _, _ := q.Enqueue(Job{DedupKey: "persist"})
	q.Close()

	q2, _ := Open(opts)
	defer q2.Close()
	id2, ins2, _ := q2.Enqueue(Job{DedupKey: "persist"})
	if ins2 || id1 != id2 {
		t.Fatalf("重启后去重失效: id1=%s id2=%s ins2=%v", id1, id2, ins2)
	}
	id3, _, _ := q2.Enqueue(Job{DedupKey: "new-key"})
	if id3 == id1 {
		t.Fatalf("重启后任务 ID 被复用: %s", id3)
	}
	t.Logf("  重启后去重命中(%s), 新任务分配新 ID(%s) OK", id2, id3)
}

func TestDedupCapacityEviction(t *testing.T) {
	t.Log("业务: 幂等去重容量 / 用例: 容量满时按 LRU 淘汰已终结任务, 重试中的任务不被误淘汰, 无可淘汰项时拒绝入队")
	dir := t.TempDir()
	opts := testOptions(dir)
	opts.DedupCapacity = 3
	q, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	// 占满 3 个槽位：k1 已确认(可淘汰), k2 重试中(不可淘汰), k3 已确认(可淘汰)。
	q.Enqueue(Job{DedupKey: "k1", MaxAttempts: 1})
	q.Enqueue(Job{DedupKey: "k2", MaxAttempts: 5})
	q.Enqueue(Job{DedupKey: "k3", MaxAttempts: 1})
	d, _ := q.Pull("g")
	q.Ack("g", d.Job.ID) // k1 done
	d, _ = q.Pull("g")
	k2 := d.Job.ID
	q.Nack("g", k2, FailRetryable, errors.New("later")) // k2 重试中
	d, _ = q.Pull("g")
	q.Ack("g", d.Job.ID) // k3 done

	// 入队 k4：应淘汰最久未使用的可淘汰键 k1，而不是重试中的 k2。
	if _, _, err := q.Enqueue(Job{DedupKey: "k4"}); err != nil {
		t.Fatalf("有可淘汰项时不应失败: %v", err)
	}
	if _, ins := mustEnqueue(t, q, "k2"); ins {
		t.Fatal("重试中的 k2 的去重记录不得被淘汰")
	}
	if _, ins := mustEnqueue(t, q, "k3"); ins {
		t.Fatal("k3 的去重记录不应被淘汰(它比 k1 更新)")
	}
	if _, ins, err := q.Enqueue(Job{DedupKey: "k1"}); err != nil || !ins {
		t.Fatalf("k1 应已被淘汰, 可重新入队: ins=%v err=%v", ins, err)
	}
	t.Log("  淘汰最久未使用的已完成任务 k1, 重试中的 k2 与较新的 k3 均保留 OK")

	// 另起一个容量为 2 的队列，让所有键都不可淘汰（全部在投递中），
	// 再入队应返回 ErrDedupFull。
	q2, err := Open(Options{Dir: t.TempDir(), DedupCapacity: 2, Lease: time.Minute, MaxAttempts: 5, BackoffBase: time.Minute, BackoffMax: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	defer q2.Close()
	q2.Enqueue(Job{DedupKey: "busy1"})
	q2.Enqueue(Job{DedupKey: "busy2"})
	// busy1、busy2 都拉出来不确认（inflight，不可淘汰）。
	q2.Pull("g")
	q2.Pull("g")
	_, _, err = q2.Enqueue(Job{DedupKey: "busy3"})
	if !errors.Is(err, ErrDedupFull) {
		t.Fatalf("全部键不可淘汰时应返回 ErrDedupFull, got %v", err)
	}
	t.Log("  所有任务都在投递中时拒绝入队(ErrDedupFull) OK")
}

func mustEnqueue(t *testing.T, q *Queue, key string) (string, bool) {
	t.Helper()
	id, ins, err := q.Enqueue(Job{DedupKey: key})
	if err != nil {
		t.Fatalf("enqueue %s: %v", key, err)
	}
	return id, ins
}

