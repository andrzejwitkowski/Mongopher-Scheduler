package store

import (
	"mongopher-scheduler/retry"
	"time"
)

type TaskStatus string

const (
    StatusNew        TaskStatus = "NEW"
    StatusInProgress TaskStatus = "IN_PROGRESS"
    StatusDone       TaskStatus = "DONE"
    StatusException  TaskStatus = "EXCEPTION"
    StatusRetrying   TaskStatus = "RETRYING"
)

const (
    RetryStrategyLinear      = "linear"
    RetryStrategyBackpressure = "backpressure"
)

type TaskHistory struct {
    Status    TaskStatus `bson:"status"`
    Timestamp time.Time  `bson:"timestamp"`
    Error     string     `bson:"error,omitempty"`
}

type RetryConfig struct {
    MaxRetries   int    `bson:"max_retries"`
    StrategyType string `bson:"strategy_type"`
    BaseDelay    int    `bson:"base_delay"`    // in milliseconds
    Attempts     int    `bson:"attempts"`
}

func (rc RetryConfig) GetStrategy() retry.RetryStrategy {
	switch rc.StrategyType {
	case RetryStrategyLinear:
		return &retry.LinearRetry{DelayMillis: rc.BaseDelay}
	case RetryStrategyBackpressure:
		return &retry.BackpressureRetry{BaseMillis: rc.BaseDelay}
	default:
		return &retry.LinearRetry{DelayMillis: 1000}
	}
}

type TaskParameter interface {
	ToMap() (map[string]interface{}, error)
}

type Task[T any, ID any] struct {
    ID          ID                 `bson:"_id,omitempty"`
    Name        string            `bson:"name"`
    Status      TaskStatus        `bson:"status"`
    CreatedAt   time.Time         `bson:"created_at"`
    ExecutedAt  *time.Time        `bson:"executed_at,omitempty"`
    ScheduledAt *time.Time        `bson:"scheduled_at,omitempty"`
    Params      T                 `bson:"params,omitempty"`
    StackTrace  string            `bson:"stack_trace,omitempty"`
    RetryConfig RetryConfig       `bson:"retry_config"`
    History     []TaskHistory     `bson:"history"`
}