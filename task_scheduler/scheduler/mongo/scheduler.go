package mongo

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/logging"
	"github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/scheduler"
	"github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/shared"
	"github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/store"
	"github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/store/mongo"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	mongo_client "go.mongodb.org/mongo-driver/mongo"
)

var logger = logging.GetLogger().Sugar()

type MongoTaskParameter primitive.M

func (mp MongoTaskParameter) ToMap() (map[string]interface{}, error) {
    return map[string]interface{}(mp), nil
}

type MongoTaskScheduler struct {
	context *context.Context
	cancelFunc context.CancelFunc
    store *mongo.MongoStore
    handlers map[string]func(scheduler.Task) error
	mu       sync.RWMutex  
}

func NewMongoTaskScheduler(client *mongo_client.Client, dbName string) *MongoTaskScheduler {
    return &MongoTaskScheduler{
        store: mongo.NewMongoStore(client, dbName),
        handlers: make(map[string]func(scheduler.Task) error),
    }
}

// RegisterHandler registers a new task handler
func (ts *MongoTaskScheduler) RegisterHandler(name string, handler func(scheduler.Task) error) {
	ts.handlers[name] = handler
}

// StartScheduler begins processing tasks
func (ts *MongoTaskScheduler) StartScheduler(ctx context.Context) {

	// Create a new context with a cancel function
	ctx, ts.cancelFunc = context.WithCancel(ctx)
	ts.context = &ctx
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

func (ts *MongoTaskScheduler) StopScheduler() {
	if ts.cancelFunc != nil {
		ts.cancelFunc()
	}
}

// RegisterTask creates a new task in the database
func (ts *MongoTaskScheduler) RegisterTask(name string, params map[string]interface{}, scheduledAt *time.Time) (scheduler.Task, error) {
	task := mongo.MongoTask{
		Name:        name,
		Status:      store.StatusNew,
		CreatedAt:   time.Now(),
		Params:      params,
		ScheduledAt: scheduledAt,
		RetryConfig: store.RetryConfig{
			MaxRetries:   5,
			StrategyType: store.RetryStrategyLinear,
			BaseDelay:    1000,
			Attempts:     0,
		},
		History: []store.TaskHistory{}, // Initialize history as empty array
	}

	task_result, err := ts.store.InsertTask(context.Background(), task)
	return scheduler.Task(task_result), err
}

func (ts *MongoTaskScheduler) FindTasksInStatus(ctx context.Context, task_status store.TaskStatus) ([]scheduler.Task, error) {
	tasks, err := ts.store.FindTasksInStatus(ctx, task_status)
	if err != nil {
		return nil, err
	}

	converted_tasks := make([]scheduler.Task, len(tasks))
	for i, task := range tasks {
		converted_tasks[i] = scheduler.Task(task)
	}
	return converted_tasks, nil
}

func mapToBSON(m map[string]interface{}) (bson.M, error) {
    // First convert the map to BSON bytes
    data, err := bson.Marshal(m)
    if err != nil {
        return nil, err
    }
    
    // Then unmarshal back to bson.M
    var result bson.M
    err = bson.Unmarshal(data, &result)
    if err != nil {
        return nil, err
    }
    
    return result, nil
}

func (ts *MongoTaskScheduler) processTasks(ctx context.Context) {
	// Find all tasks that are ready to be executed and update to IN_PROGRESS
	tasks := shared.Must(ts.store.FindTasksDueAndUpdateToInProgress(ctx))

	// Launch processing without waiting
	for _, task := range tasks {
		go func(t mongo.MongoTask) {
			ts.processTaskWithRetry(ctx, task.ID)
		}(task)
	}
}

func (ts *MongoTaskScheduler) getHandler(taskName string) (func(scheduler.Task) error, bool) {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	handler, exists := ts.handlers[taskName]
	return handler, exists
}

func (ts *MongoTaskScheduler) processTaskWithRetry(ctx context.Context, taskId primitive.ObjectID) {

	task := shared.Must(ts.store.GetTaskByID(ctx, taskId))

	handler, exists := ts.getHandler(task.Name)

	if !exists {
		ts.store.UpdateTaskState(ctx, task.ID, store.StatusException, fmt.Sprintf("No handler found for TaskName: %s", task.Name), 0, nil)
		return
	}

	if task.RetryConfig.Attempts <= task.RetryConfig.MaxRetries {
		if (task.Status != store.StatusInProgress){
			logger.Debugf("Marking task %s (current status: %s) as IN_PROGRESS", task.ID.Hex(), task.Status)
			if err := ts.store.UpdateTaskState(ctx, task.ID, store.StatusInProgress, "", task.RetryConfig.Attempts, nil); err != nil {
				logger.Errorf("Error marking task as IN_PROGRESS: %v", err)
				return
			}
		}

		err := handler(*task)

		if err == nil {
			ts.store.UpdateTaskState(ctx, task.ID, store.StatusDone, "", task.RetryConfig.Attempts, nil)
			return
		}

		next_attempt := task.RetryConfig.Attempts + 1
		delay := task.RetryConfig.GetStrategy().NextDelay(task.RetryConfig.Attempts)
		nextExecution := time.Now().Add(delay)

		// Update task state for retry
		if err := ts.store.UpdateTaskState(ctx, task.ID, store.StatusRetrying, err.Error(), next_attempt, &nextExecution); err != nil {
			logger.Errorf("Error updating task state: %v", err)
		}
	} else {
		ts.store.UpdateTaskState(ctx, task.ID, store.StatusException, "Max retries exceeded", task.RetryConfig.Attempts, nil)
	}
}