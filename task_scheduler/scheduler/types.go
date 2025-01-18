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
	StopScheduler()
	RegisterTask(name string, params store.TaskParameter, scheduledAt *time.Time) (*store.Task[T, ID], error)

	FindTasksInStatus(ctx context.Context, task_status store.TaskStatus) ([]store.Task[T, ID], error)
}

type WaitForTasksOptions struct {
	MaxRetries int
	Timeout    time.Duration
	RetryDelay time.Duration
}
type TaskStatusObserver interface {
	WaitForAllTasksToBeDone() (bool, error)
	WaitForAllTasksToBeDoneWithOptions (options WaitForTasksOptions) (bool, error)

	WaitForAllTasksToBeInStatusWithOptions (status store.TaskStatus, options WaitForTasksOptions) (bool, error)
	WaitForAllTasksToBeInStatus (status store.TaskStatus) (bool, error)
}

func DefaultWaitForTasksOptions() WaitForTasksOptions {
	return WaitForTasksOptions{
		MaxRetries: 10,
		Timeout:    30 * time.Second,
		RetryDelay: 1 * time.Second,
	}
}