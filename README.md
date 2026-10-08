# durable-job-queue-go

单机本地任务队列，模拟任务的**入队、投递、确认、失败重试与死信**，
在重复投递、并发消费、进程重启三类场景下保证状态可预期。

## 快速开始

```go
q, _ := queue.Open(queue.Options{Dir: "./data", MaxAttempts: 3})
defer q.Close()

id, inserted, _ := q.Enqueue(queue.Job{DedupKey: "order-1", Payload: []byte("...")})
// inserted=false 表示 DedupKey 命中去重，返回的是已有任务 ID

d, _ := q.Pull("groupA")      // 拉取一次投递（租约内同组独占）
if err := handle(d.Job); err != nil {
    q.Nack("groupA", d.Job.ID, queue.FailRetryable, err) // 可重试失败
} else {
    q.Ack("groupA", d.Job.ID) // 确认，幂等
}
dead := q.DeadLetters("groupA") // 单独查询死信
```

## 投递语义（至少一次）

- `Pull` 把任务置为 `inflight` 并附加**租约**（`Options.Lease`）。租约内同一任务
  不会被同组其他消费者拿到；租约超时未 `Ack`，任务自动变为可再次投递。
- 因此**重复投递是常态**，消费端必须幂等；队列侧通过 `DedupKey` 保证同一逻辑任务
  只入队一次，通过 `Ack` 幂等保证重复确认安全。
- 已 `Ack` 的任务在该组**绝不再投递**（包括重启之后）。
- 进程重启时，所有未确认的 `inflight` 一律回到 `pending` 重新投递——
  未确认任务宁可重投，绝不悄悄丢失。

## 消费组与并发

- 每个消费组对每个任务有独立的投递状态机：
  `pending → inflight → done`，失败时 `inflight → retry → inflight …→ dead`。
- 同组多个消费者并发 `Pull`：由互斥锁 + 租约保证一个任务同一时刻只落到一个消费者。
- 不同分组各自独立收到**全量**任务（广播语义），互不影响进度。

## 失败、重试与死信

`Nack` 时必须给出可区分的失败原因 `FailKind`：

| 失败原因 | 行为 |
|---|---|
| `FailRetryable` | 进入退避重试；累计投递次数达到 `MaxAttempts` 后进死信 |
| `FailFatal` | 不重试，直接进死信 |

- 退避策略：指数退避 `BackoffBase * 2^(n-1)`（n 为已投递次数），封顶 `BackoffMax`，
  确定且可解释，退避窗口内任务不会被投出。
- `MaxAttempts` 可按任务设置（`Job.MaxAttempts`），缺省用 `Options.MaxAttempts`。
- 死信通过 `DeadLetters(group)` 单独查询，包含投递次数、最后失败原因与错误信息；
  死信任务不再被投递。

## 幂等去重与容量约束

- 入队必须携带 `DedupKey`；命中去重索引时返回已有任务 ID，不产生第二条任务。
- 去重索引容量受 `Options.DedupCapacity` 约束，**不会无限增长**。
- 容量满时的淘汰策略（确定且可解释）：**LRU**——淘汰“最久未使用”的键，
  但**仅当对应任务在所有消费组都已终结（done/dead）时才允许淘汰**；
  仍在 pending/inflight/retry 的任务（包括重试中的任务）不会被误淘汰。
- 若所有键都不可淘汰，`Enqueue` 返回 `ErrDedupFull`，由调用方决策（稍后重试或扩容）。
- 去重索引随快照一起持久化，重启后淘汰顺序与重启前一致。

## 持久化与取舍

- 每次状态变更（入队/投递/确认/失败）后整体写 JSON 快照：
  写临时文件 → fsync → rename，崩溃时只会看到旧快照或新快照，不会出现半写状态。
- 快照包含：任务列表与全局顺序、单调递增的任务 ID 序列（重启后 ID 不复用）、
  各消费组的投递状态（含重试次数、退避到期时间、死信）、去重索引（含 LRU 顺序）。
- 恢复时不引入重复或错位：`inflight → pending` 重置、任务 ID 序列持久化、
  已确认/死信状态原样恢复。
- 取舍：全量快照实现简单、恢复逻辑确定，适合单机模拟与中小任务量；
  任务量大时写放大明显，真实系统应换成 WAL + 定期快照。

## 本地验证

```bash
# 运行全部测试（日志会标明每个用例覆盖的业务场景与结果）
go test ./queue/ -v

# 带竞态检测
go test ./queue/ -race

# 运行端到端演示（入队→消费→重试→死信→模拟重启恢复）
go run ./cmd/demo
```

测试覆盖的业务场景：

| 业务 | 用例 |
|---|---|
| 投递与确认（至少一次） | 租约超时重复投递、确认后不再投递、重复确认幂等、错误可区分 |
| 幂等去重 | 相同 DedupKey 只入队一次、重启后仍去重、任务 ID 不复用 |
| 消费组并发消费 | 同组 8 消费者并发拉取 50 任务互不重复、不同分组各拿全量 |
| 失败重试与死信 | 退避窗口内不重投、退避到期重投、超上限进死信、致命失败直接死信、死信可查询 |
| 重启恢复 | 未确认任务重投、重试次数/死信保留、已确认不重投、二次重启无重复 |
| 去重容量 | LRU 淘汰已终结任务、重试中任务不被误淘汰、全忙时返回 ErrDedupFull |
