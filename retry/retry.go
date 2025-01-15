package retry

import (
	"time"
)

type RetryStrategy interface {
	NextDelay(attempt int) time.Duration
}

type LinearRetry struct {
	DelayMillis int
}

func (lr LinearRetry) NextDelay(attempt int) time.Duration {
	return time.Duration(lr.DelayMillis) * time.Millisecond
}

type BackpressureRetry struct {
	BaseMillis int
}

func (br BackpressureRetry) NextDelay(attempt int) time.Duration {
	delay := br.BaseMillis * (attempt * attempt)
	return time.Duration(delay) * time.Millisecond
}
