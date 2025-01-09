package task_scheduler

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"mongopher-scheduler/retry"
)

type TaskStatus string

const (
	StatusNew       TaskStatus = "NEW"
	StatusInProgress TaskStatus = "IN_PROGRESS"
	StatusDone      TaskStatus = "DONE"
	StatusException TaskStatus = "EXCEPTION"
	StatusRetrying  TaskStatus = "RETRYING"
)

type TaskHistory struct {
	Status    TaskStatus  `bson:"status"`
	Timestamp time.Time   `bson:"timestamp"`
	Error     string      `bson:"error,omitempty"`
}

type Task struct {
	ID          primitive.ObjectID `bson:"_id,omitempty"`
	Name        string             `bson:"name"`
	Status      TaskStatus         `bson:"status"`
	CreatedAt   time.Time          `bson:"created_at"`
	ExecutedAt  *time.Time         `bson:"executed_at,omitempty"`
	ScheduledAt *time.Time         `bson:"scheduled_at,omitempty"`
	Params      bson.M             `bson:"params,omitempty"`
	StackTrace  string             `bson:"stack_trace,omitempty"`
	RetryConfig RetryConfig        `bson:"retry_config"`
	History     []TaskHistory      `bson:"history"`
}

const (
	RetryStrategyLinear      = "linear"
	RetryStrategyBackpressure = "backpressure"
)

type RetryConfig struct {
	MaxRetries    int    `bson:"max_retries"`
	StrategyType  string `bson:"strategy_type"` // Use RetryStrategyLinear or RetryStrategyBackpressure
	BaseDelay     int    `bson:"base_delay"`    // in milliseconds
	Attempts      int    `bson:"attempts"`
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

type TaskHandler func(*Task) error

type TaskScheduler struct {
	collection  *mongo.Collection
	handlers    map[string]TaskHandler
}

var (
	instance *TaskScheduler
	once     sync.Once
)

// NewTaskScheduler creates a new TaskScheduler instance
func NewTaskScheduler(client *mongo.Client, dbName string) (*TaskScheduler, error) {
	collection := client.Database(dbName).Collection("tasks")
	return &TaskScheduler{
		collection: collection,
		handlers:   make(map[string]TaskHandler),
	}, nil
}

// GetInstance returns a singleton instance of TaskScheduler
func GetInstance(client *mongo.Client, dbName string) (*TaskScheduler, error) {
	var initErr error
	once.Do(func() {
		instance, initErr = NewTaskScheduler(client, dbName)
	})
	return instance, initErr
}

// RegisterHandler registers a new task handler
func (ts *TaskScheduler) RegisterHandler(name string, handler TaskHandler) {
	ts.handlers[name] = handler
}

// StartScheduler begins processing tasks
func (ts *TaskScheduler) StartScheduler(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				ts.processTasks(ctx)
				time.Sleep(1 * time.Second)
			}
		}
	}()
}

// RegisterTask creates a new task in the database
func (ts *TaskScheduler) RegisterTask(name string, params bson.M, scheduledAt *time.Time) (*Task, error) {
	task := Task{
		Name:        name,
		Status:      StatusNew,
		CreatedAt:   time.Now(),
		Params:      params,
		ScheduledAt: scheduledAt,
		RetryConfig: RetryConfig{
			MaxRetries:   5,
			StrategyType: RetryStrategyLinear,
			BaseDelay:    1000,
			Attempts:     0,
		},
		History: []TaskHistory{}, // Initialize history as empty array
	}

	result, err := ts.collection.InsertOne(context.Background(), task)
	if err != nil {
		return nil, err
	}

	task.ID = result.InsertedID.(primitive.ObjectID)
	return &task, nil
}

func (ts *TaskScheduler) processTasks(ctx context.Context) {
	// Find all tasks that are ready to be executed
	filter := bson.M{
		"status": bson.M{"$in": []TaskStatus{StatusNew, StatusRetrying}},
		"$or": []bson.M{
			{"scheduled_at": bson.M{"$exists": false}},
			{"scheduled_at": bson.M{"$lte": time.Now()}},
		},
	}

	cursor, err := ts.collection.Find(ctx, filter)
	if err != nil {
		log.Printf("Error finding tasks: %v", err)
		return
	}
	defer cursor.Close(ctx)

	var tasks []Task
	if err := cursor.All(ctx, &tasks); err != nil {
		log.Printf("Error decoding tasks: %v", err)
		return
	}

	// Process each task
	for _, task := range tasks {
		// Update status to IN_PROGRESS
		if err := ts.updateTaskState(ctx, task.ID, StatusInProgress, "", 0, nil); err != nil {
			log.Printf("Error updating task status: %v", err)
			continue
		}

		// Process task with retry logic
		ts.processTaskWithRetry(ctx, task)
	}
}

func (ts *TaskScheduler) processTaskWithRetry(ctx context.Context, task Task) {
	handler, exists := ts.handlers[task.Name]
	if !exists {
		ts.updateTaskState(ctx, task.ID, StatusException, fmt.Sprintf("No handler found for TaskName: %s", task.Name), 0, nil)
		return
	}

	for task.RetryConfig.Attempts <= task.RetryConfig.MaxRetries {
		err := handler(&task)
		if err == nil {
			ts.updateTaskState(ctx, task.ID, StatusDone, "", task.RetryConfig.Attempts, nil)
			return
		}

		task.RetryConfig.Attempts++
		delay := task.RetryConfig.GetStrategy().NextDelay(task.RetryConfig.Attempts)
		nextExecution := time.Now().Add(delay)

		// Update task state for retry
		if err := ts.updateTaskState(ctx, task.ID, StatusRetrying, err.Error(), task.RetryConfig.Attempts, &nextExecution); err != nil {
			log.Printf("Error updating task state: %v", err)
		}

		time.Sleep(delay)
	}

	ts.updateTaskState(ctx, task.ID, StatusException, "Max retries exceeded", task.RetryConfig.Attempts, nil)
}

func (ts *TaskScheduler) updateTaskState(ctx context.Context, id primitive.ObjectID, status TaskStatus, errorMsg string, retryAttempts int, scheduledAt *time.Time) error {
	historyEntry := TaskHistory{
		Status:    status,
		Timestamp: time.Now(),
		Error:     errorMsg,
	}

	update := bson.M{
		"$set": bson.M{
			"status": status,
			"retry_config.attempts": retryAttempts,
		},
		"$push": bson.M{"history": historyEntry},
	}

	if scheduledAt != nil {
		update["$set"].(bson.M)["scheduled_at"] = *scheduledAt
	}

	_, err := ts.collection.UpdateByID(ctx, id, update)
	return err
}
