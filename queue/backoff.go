package queue

import "time"

// backoff 计算第 attempt 次重试前的等待时长（attempt 从 1 开始）。
// 策略：base * 2^(attempt-1)，封顶 max，保证确定且可解释。
func backoff(base, max time.Duration, attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := base
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= max {
			return max
		}
	}
	if d > max {
		return max
	}
	return d
}
