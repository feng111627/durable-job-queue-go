// Package queue 实现一套本地持久化任务队列，
// 支持入队、投递、确认、失败重试、死信、消费组与幂等去重。
package queue

import (
	"errors"
	"time"
)

// 错误定义。
var (
	ErrJobNotFound    = errors.New("queue: job not found")
	ErrNotInflight    = errors.New("queue: job is not inflight in this group")
	ErrDedupFull      = errors.New("queue: dedup capacity reached and no evictable entry")
	ErrQueueClosed    = errors.New("queue: queue is closed")
	ErrEmptyDedupKey  = errors.New("queue: dedup key must not be empty")
	ErrInvalidMaxTry  = errors.New("queue: max attempts must be >= 1")
)

// FailKind 区分失败原因，决定进入重试还是直接死信。
type FailKind int

const (
	// FailRetryable 可重试失败：按退避策略重投，超过次数上限进死信。
	FailRetryable FailKind = iota
	// FailFatal 致命失败：不重试，直接进死信。
	FailFatal
)

func (k FailKind) String() string {
	switch k {
	case FailRetryable:
		return "retryable"
	case FailFatal:
		return "fatal"
	default:
		return "unknown"
	}
}

// DeliveryStatus 表示一个任务在某个消费组内的投递状态。
type DeliveryStatus string

const (
	StatusPending  DeliveryStatus = "pending"   // 等待投递
	StatusInflight DeliveryStatus = "inflight"  // 已投递未确认
	StatusRetry    DeliveryStatus = "retry"     // 等待退避后重投
	StatusDone     DeliveryStatus = "done"      // 已确认
	StatusDead     DeliveryStatus = "dead"      // 死信
)

// Job 是入队的任务本体，被所有消费组共享。
type Job struct {
	ID          string    `json:"id"`
	DedupKey    string    `json:"dedup_key"`
	Payload     []byte    `json:"payload"`
	MaxAttempts int       `json:"max_attempts"`
	CreatedAt   time.Time `json:"created_at"`
}

// Delivery 是 Pull 返回给消费者的一次投递。
type Delivery struct {
	Job      Job      `json:"job"`
	Group    string   `json:"group"`
	Attempts int      `json:"attempts"` // 本次是第几次投递（从 1 开始）
	Status   DeliveryStatus `json:"status"`
}

// DeadLetter 描述一条死信，可单独查询。
type DeadLetter struct {
	Job        Job      `json:"job"`
	Group      string   `json:"group"`
	Attempts   int      `json:"attempts"`
	LastFail   FailKind `json:"last_fail"`
	LastError  string   `json:"last_error"`
	DeadAt     time.Time `json:"dead_at"`
}

// Options 是队列配置。
type Options struct {
	Dir           string        // 持久化目录
	DedupCapacity int           // 幂等去重记录容量上限
	Lease         time.Duration // 投递租约：超时未确认自动重投
	MaxAttempts   int           // 默认最大投递次数（任务可覆盖）
	BackoffBase   time.Duration // 退避基数：第 n 次重试等待 BackoffBase << (n-1)
	BackoffMax    time.Duration // 退避上限
}

func (o *Options) withDefaults() {
	if o.DedupCapacity <= 0 {
		o.DedupCapacity = 1024
	}
	if o.Lease <= 0 {
		o.Lease = 30 * time.Second
	}
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = 3
	}
	if o.BackoffBase <= 0 {
		o.BackoffBase = 100 * time.Millisecond
	}
	if o.BackoffMax <= 0 {
		o.BackoffMax = 30 * time.Second
	}
}
