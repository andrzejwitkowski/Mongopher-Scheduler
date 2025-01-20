package mongo

import (
	"context"
	"fmt"
	"log"
	"github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/shared"
	"github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/store"
	"github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/store/mongo"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	mongo_client "go.mongodb.org/mongo-driver/mongo"
)

type MongoTaskParameter primitive.M

func (mp MongoTaskParameter) ToMap() (map[string]interface{}, error) {
    return map[string]interface{}(mp), nil
}

type MongoTaskHandler func(mongo.MongoTask) error

type MongoTaskScheduler struct {
	context *context.Context
	cancelFunc context.CancelFunc
    store *mongo.MongoStore
    handlers map[string]MongoTaskHandler
	mu       sync.RWMutex  
}

func NewMongoTaskScheduler(client *mongo_client.Client, dbName string) *MongoTaskScheduler {
    return &MongoTaskScheduler{
        store: mongo.NewMongoStore(client, dbName),
        handlers: make(map[string]MongoTaskHandler),
    }
}

// RegisterHandler registers a new task handler
func (ts *MongoTaskScheduler) RegisterHandler(name string, handler MongoTaskHandler) {
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
func (ts *MongoTaskScheduler) RegisterTask(name string, params store.TaskParameter, scheduledAt *time.Time) (*mongo.MongoTask, error) {
	task := mongo.MongoTask{
		Name:        name,
		Status:      store.StatusNew,
		CreatedAt:   time.Now(),
		Params:      shared.Must(mapToBSON(shared.Must(params.ToMap()))),
		ScheduledAt: scheduledAt,
		RetryConfig: store.RetryConfig{
			MaxRetries:   5,
			StrategyType: store.RetryStrategyLinear,
			BaseDelay:    1000,
			Attempts:     0,
		},
		History: []store.TaskHistory{}, // Initialize history as empty array
	}

	return ts.store.InsertTask(context.Background(), task)
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

func (ts *MongoTaskScheduler) getHandler(taskName string) (MongoTaskHandler, bool) {
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
			log.Printf("{GoroutineID: %d} Marking task %s (current status: %s) as IN_PROGRESS", 
				shared.GoroutineID(), task.ID.Hex(), task.Status)
			if err := ts.store.UpdateTaskState(ctx, task.ID, store.StatusInProgress, "", task.RetryConfig.Attempts, nil); err != nil {
				log.Printf("Error marking task as IN_PROGRESS: %v", err)
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
			log.Printf("Error updating task state: %v", err)
		}
	} else {
		ts.store.UpdateTaskState(ctx, task.ID, store.StatusException, "Max retries exceeded", task.RetryConfig.Attempts, nil)
	}
}

func (ts *MongoTaskScheduler) FindTasksInStatus(ctx context.Context, task_status store.TaskStatus) ([]store.Task[bson.M, primitive.ObjectID], error) {
	mongoTasks, err := ts.store.FindTasksInStatus(ctx, task_status)
	if err != nil {
		return nil, err
	}
	tasks := make([]store.Task[bson.M, primitive.ObjectID], len(mongoTasks))
	for i, task := range mongoTasks {
		tasks[i] = store.Task[bson.M, primitive.ObjectID](task)
	}
	return tasks, nil
}