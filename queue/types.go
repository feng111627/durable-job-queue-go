// Package queue 提供单机持久化任务队列：入队、投递、确认、失败重试与死信。
package queue

import (
	"errors"
	"time"
)

// FailureKind 区分消费失败的原因，决定任务进入重试还是直接进死信。
type FailureKind int

const (
	// FailureRetryable 可重试失败：按退避策略重新投递，超过上限进死信。
	FailureRetryable FailureKind = iota
	// FailureFatal 不可重试失败：直接进死信。
	FailureFatal
)

func (k FailureKind) String() string {
	switch k {
	case FailureRetryable:
		return "retryable"
	case FailureFatal:
		return "fatal"
	default:
		return "unknown"
	}
}

// 队列对外暴露的错误。
var (
	// ErrDedupFull 去重表已满且没有可淘汰的记录（所有记录对应任务仍在处理中）。
	ErrDedupFull = errors.New("queue: dedup table full, no evictable entry")
	// ErrUnknownJob 任务在该消费组中不存在或已被确认。
	ErrUnknownJob = errors.New("queue: unknown job for group")
	// ErrStaleToken 投递令牌已失效（任务已被重新投递给其他消费者）。
	ErrStaleToken = errors.New("queue: stale delivery token")
	// ErrClosed 队列已关闭。
	ErrClosed = errors.New("queue: closed")
)

// Job 是入队的任务。
type Job struct {
	ID         string    `json:"id"`
	Key        string    `json:"key"` // 幂等键，空字符串表示不去重
	Payload    []byte    `json:"payload"`
	EnqueuedAt time.Time `json:"enqueued_at"`
}

// jobState 是任务在某个消费组内的状态。
type jobState int

const (
	statePending  jobState = iota // 等待投递
	stateInflight                 // 已投递未确认（租约中）
	stateAcked                    // 已确认
	stateDead                     // 已进入死信
)

// deliveryRecord 记录一个任务在一个消费组内的投递状态。
type deliveryRecord struct {
	State     jobState  `json:"state"`
	Attempts  int       `json:"attempts"`   // 已投递次数
	Consumer  string    `json:"consumer"`   // 当前持有租约的消费者
	Token     string    `json:"token"`      // 本次投递的令牌
	VisibleAt time.Time `json:"visible_at"` // inflight: 租约到期时间；pending: 下次可投递时间
	LastError string    `json:"last_error"`
}

// Delivery 是投递给消费者的一次任务。
type Delivery struct {
	Job     Job
	Group   string
	Token   string // 确认/失败时需带回，防止旧消费者误操作
	Attempt int    // 第几次投递（从 1 开始）
}

// DeadLetter 是进入死信的任务记录。
type DeadLetter struct {
	Job      Job
	Group    string
	Attempts int
	Reason   string
	FailedAt time.Time
}

// Options 是队列配置。
type Options struct {
	MaxRetries        int           // 可重试失败的最大重试次数，超过进死信
	BaseBackoff       time.Duration // 退避基数，第 n 次重试等待 BaseBackoff * 2^(n-1)
	MaxBackoff        time.Duration // 退避上限
	VisibilityTimeout time.Duration // 投递租约时长，超时未确认自动重新投递
	DedupCapacity     int           // 幂等去重表容量上限
	Now               func() time.Time
}

func (o *Options) withDefaults() Options {
	out := *o
	if out.MaxRetries <= 0 {
		out.MaxRetries = 3
	}
	if out.BaseBackoff <= 0 {
		out.BaseBackoff = 100 * time.Millisecond
	}
	if out.MaxBackoff <= 0 {
		out.MaxBackoff = 30 * time.Second
	}
	if out.VisibilityTimeout <= 0 {
		out.VisibilityTimeout = 5 * time.Second
	}
	if out.DedupCapacity <= 0 {
		out.DedupCapacity = 1024
	}
	if out.Now == nil {
		out.Now = time.Now
	}
	return out
}
