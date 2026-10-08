// demo 演示本地任务队列的完整生命周期：
// 入队 -> 两个消费组各自消费 -> 失败重试 -> 死信 -> 模拟重启恢复。
package main

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/feng111627/durable-job-queue-go/queue"
)

func main() {
	dir := "demo-data"
	opts := queue.Options{
		Dir:           dir,
		DedupCapacity: 16,
		Lease:         2 * time.Second,
		MaxAttempts:   3,
		BackoffBase:   100 * time.Millisecond,
		BackoffMax:    500 * time.Millisecond,
	}

	q, err := queue.Open(opts)
	if err != nil {
		log.Fatal(err)
	}

	// 1. 入队（含一次重复入队，演示幂等去重）。
	id1, ins1, _ := q.Enqueue(queue.Job{DedupKey: "order-1", Payload: []byte("pay order 1")})
	_, ins2, _ := q.Enqueue(queue.Job{DedupKey: "order-1", Payload: []byte("pay order 1")})
	id2, _, _ := q.Enqueue(queue.Job{DedupKey: "order-2", Payload: []byte("pay order 2"), MaxAttempts: 2})
	fmt.Printf("[enqueue] %s inserted=%v; 重复入队 order-1 inserted=%v(去重生效); %s\n", id1, ins1, ins2, id2)

	// 2. 消费组 A：处理 order-1 并确认；order-2 两次失败进死信。
	for {
		d, _ := q.Pull("groupA")
		if d == nil {
			break
		}
		fmt.Printf("[groupA] 投递 %s attempt=%d payload=%q\n", d.Job.ID, d.Attempts, d.Job.Payload)
		if d.Job.ID == id1 {
			q.Ack("groupA", d.Job.ID)
			fmt.Printf("[groupA] 确认 %s\n", d.Job.ID)
		} else {
			q.Nack("groupA", d.Job.ID, queue.FailRetryable, errors.New("支付网关超时"))
			time.Sleep(600 * time.Millisecond) // 等退避到期
		}
	}
	for _, dl := range q.DeadLetters("groupA") {
		fmt.Printf("[groupA] 死信 %s attempts=%d 原因=%s(%s)\n", dl.Job.ID, dl.Attempts, dl.LastFail, dl.LastError)
	}

	// 3. 消费组 B 只拉取不确认，然后“崩溃”，演示重启后重新投递。
	d, _ := q.Pull("groupB")
	if d != nil {
		fmt.Printf("[groupB] 投递 %s 但不确认, 模拟进程退出\n", d.Job.ID)
	} else {
		fmt.Println("[groupB] 无可投递任务(上次运行已全部确认, 说明已确认任务不会被重投)")
	}
	q.Close()

	q2, err := queue.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer q2.Close()
	fmt.Println("[restart] 队列已重启, 状态从磁盘恢复")
	for {
		d, _ := q2.Pull("groupB")
		if d == nil {
			break
		}
		fmt.Printf("[groupB] 重启后重投 %s attempt=%d, 确认之\n", d.Job.ID, d.Attempts)
		q2.Ack("groupB", d.Job.ID)
	}
	fmt.Printf("[restart] groupA 死信仍在: %d 条\n", len(q2.DeadLetters("groupA")))
}
