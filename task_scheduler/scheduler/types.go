package scheduler

import (
	"context"
	"time"
	"mongopher-scheduler/task_scheduler/store"
)

type TaskHandler func(*store.Task) error
type TaskParameter interface {
	ToMap() (map[string]interface{}, error)
}

type TaskScheduler interface {
	RegisterHandler(name string, handler TaskHandler)
	StartScheduler(ctx context.Context)
	RegisterTask(name string, params TaskParameter, scheduledAt *time.Time) (*store.Task, error)
}