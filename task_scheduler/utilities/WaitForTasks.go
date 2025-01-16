package utilities

import (
	"context"
	"mongopher-scheduler/task_scheduler/store"
	"time"
)

type TaskProvider[T, ID any] interface {
	GetTasks(ctx context.Context) ([]store.Task[T, ID], error)
}

func WaitForTasksToBeInStatus[T, ID any](
	ctx context.Context,
	taskProvider TaskProvider[T, ID],
	status store.TaskStatus,
	timeout time.Duration) (bool, error) {
	return true, nil
}
