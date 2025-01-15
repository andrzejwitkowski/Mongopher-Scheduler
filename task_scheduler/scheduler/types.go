package scheduler

import (
	"context"
	"time"
	"mongopher-scheduler/task_scheduler/store"
)

type TaskHandler[T any, ID any] func(*store.Task[T, ID]) error

type TaskScheduler[T any, ID any] interface {
	RegisterHandler(name string, handler TaskHandler[T, ID])
	StartScheduler(ctx context.Context)
	RegisterTask(name string, params store.TaskParameter, scheduledAt *time.Time) (*store.Task[T, ID], error)
}