package store

import (
	"context"
	"time"
)

type TaskStore[T any, ID any] interface {
    // Core operations
    InsertTask(ctx context.Context, task Task[T, ID]) (*Task[T, ID], error)
    GetTaskByID(ctx context.Context, id ID) (*Task[T, ID], error)
    DeleteTask(ctx context.Context, id ID) error
    
    // Specific operations for scheduler
    FindTasksDue(ctx context.Context) ([]Task[T, ID], error)
    UpdateTaskStatus(ctx context.Context, id ID, status TaskStatus, errorMsg string) error
    UpdateTaskState(ctx context.Context,id ID,status TaskStatus,errorMsg string, retryAttempts int, scheduledAt *time.Time) error
    UpdateTaskRetry(ctx context.Context, id ID, attempts int, nextScheduledTime *time.Time) error
    AddTaskHistory(ctx context.Context, id ID, history TaskHistory) error

    GetAllTasks(ctx context.Context) ([]Task[T, ID], error)
    FindTasksInStatus(ctx context.Context, task_status TaskStatus) ([]Task[T, ID], error)
}
