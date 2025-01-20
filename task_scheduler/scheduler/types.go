package scheduler

import (
	"context"
	"time"
	"github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/store"
)

type Task interface {
	GetID() interface{}
	GetName() string
	GetStatus() store.TaskStatus
	GetParams() map[string]interface{}
}

type TaskScheduler interface {
	RegisterHandler(name string, handler func(Task) error)
	StartScheduler(ctx context.Context)
	StopScheduler()
	RegisterTask(name string, params map[string]interface{}, scheduledAt *time.Time) (Task, error)
	FindTasksInStatus(ctx context.Context, task_status store.TaskStatus) ([]Task, error)
	TaskStatusObserver
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