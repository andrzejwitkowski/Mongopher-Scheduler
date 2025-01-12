package store

import (
    "context"
    "time"
    "go.mongodb.org/mongo-driver/bson/primitive"
)

type TaskStore interface {
    // Core operations
    InsertTask(ctx context.Context, task Task) (*Task, error)
    GetTaskByID(ctx context.Context, id primitive.ObjectID) (*Task, error)
    DeleteTask(ctx context.Context, id primitive.ObjectID) error
    
    // Specific operations for scheduler
    FindTasksDue(ctx context.Context) ([]Task, error)
    UpdateTaskStatus(ctx context.Context, id primitive.ObjectID, status TaskStatus, errorMsg string) error
    UpdateTaskState(ctx context.Context,id primitive.ObjectID,status TaskStatus,errorMsg string, retryAttempts int,scheduledAt *time.Time) error
    UpdateTaskRetry(ctx context.Context, id primitive.ObjectID, attempts int, nextScheduledTime *time.Time) error
    AddTaskHistory(ctx context.Context, id primitive.ObjectID, history TaskHistory) error
}
