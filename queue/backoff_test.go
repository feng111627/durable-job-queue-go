package queue

import (
	"testing"
	"time"
)

func TestBackoff(t *testing.T) {
	t.Log("业务: 失败重试 / 用例: 退避时长指数增长且封顶")
	base, max := 100*time.Millisecond, 500*time.Millisecond
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 100 * time.Millisecond},
		{2, 200 * time.Millisecond},
		{3, 400 * time.Millisecond},
		{4, 500 * time.Millisecond}, // 封顶
		{10, 500 * time.Millisecond},
	}
	for _, c := range cases {
		got := backoff(base, max, c.attempt)
		if got != c.want {
			t.Fatalf("attempt=%d got=%v want=%v", c.attempt, got, c.want)
		}
		t.Logf("  attempt=%d backoff=%v OK", c.attempt, got)
	}
}
