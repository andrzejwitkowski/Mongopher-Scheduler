package inmemory

import (
	"context"
	"fmt"
	"log"
	"mongopher-scheduler/task_scheduler/shared"
	"mongopher-scheduler/task_scheduler/store"
	"mongopher-scheduler/task_scheduler/store/inmemory"
	"sync"
	"time"
)

type InMemoryTaskIDProvider struct {
	ID int
}

func (idp *InMemoryTaskIDProvider) GetNextID() int {
	idp.ID++
	return idp.ID
}

var (
	instance *InMemoryTaskIDProvider
	once     sync.Once
)

func GetInMemoryTaskIDProvider() *InMemoryTaskIDProvider {
	once.Do(func() {
		instance = &InMemoryTaskIDProvider{
			ID: 0,
		}
	})
	return instance
}

type InMemoryTaskHandler func(*store.Task[any, int]) error

type InMemoryTaskScheduler struct {
	store *inmemory.InMemoryStore
	handlers map[string]InMemoryTaskHandler
}

func NewInMemoryTaskScheduler() *InMemoryTaskScheduler {
	return &InMemoryTaskScheduler{
		store: inmemory.NewInMemoryStore(),
		handlers: make(map[string]InMemoryTaskHandler),
	}
}

// RegisterHandler registers a new task handler
func (ts *InMemoryTaskScheduler) RegisterHandler(name string, handler InMemoryTaskHandler) {
	ts.handlers[name] = handler
}

// StartScheduler begins processing tasks
func (ts *InMemoryTaskScheduler) StartScheduler(ctx context.Context) {
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
func (ts *InMemoryTaskScheduler) RegisterTask(name string, params store.TaskParameter, scheduledAt *time.Time) (*inmemory.InMemoryTask, error) {
	task := inmemory.InMemoryTask{
		ID:          GetInMemoryTaskIDProvider().GetNextID(),
		Name:        name,
		Status:      store.StatusNew,
		CreatedAt:   time.Now(),
		Params:      shared.Must(params.ToMap()),
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

func (ts *InMemoryTaskScheduler) processTasks(ctx context.Context) {
	// Find all tasks that are ready to be executed
	tasks := shared.Must(ts.store.FindTasksDue(ctx))
	log.Printf("{GoroutineID: %d}, Found %d tasks to process", shared.GoroutineID(), len(tasks))
	for _, task := range tasks {
		log.Printf("{GoroutineID: %d}, task Id: %d, status: %s", shared.GoroutineID(), task.ID, task.Status)
	}

	// Mark all tasks as IN_PROGRESS synchronously
	for _, task := range tasks {
		log.Printf("{GoroutineID: %d} Attempting to mark task %d (current status: %s) as IN_PROGRESS",
			shared.GoroutineID(), task.ID, task.Status)
		if err := ts.store.UpdateTaskState(ctx, task.ID, store.StatusInProgress, "", task.RetryConfig.Attempts, nil); err != nil {
			log.Printf("Error updating task status: %v", err)
			continue
		}
	}

	// Launch processing without waiting
	for _, task := range tasks {
		go func(t inmemory.InMemoryTask) {
			ts.processTaskWithRetry(ctx, task.ID)
		}(task)
	}
}

func (ts *InMemoryTaskScheduler) processTaskWithRetry(ctx context.Context, taskId int) {

	task := shared.Must(ts.store.GetTaskByID(ctx, taskId))

	handler, exists := ts.handlers[task.Name]
	if !exists {
		ts.store.UpdateTaskState(ctx, task.ID, store.StatusException, fmt.Sprintf("No handler found for TaskName: %s", task.Name), 0, nil)
		return
	}

	if task.RetryConfig.Attempts <= task.RetryConfig.MaxRetries {
		if (task.Status != store.StatusInProgress){
			log.Printf("{GoroutineID: %d} Marking task %d (current status: %s) as IN_PROGRESS", 
				shared.GoroutineID(), task.ID, task.Status)
			if err := ts.store.UpdateTaskState(ctx, task.ID, store.StatusInProgress, "", task.RetryConfig.Attempts, nil); err != nil {
				log.Printf("Error marking task as IN_PROGRESS: %v", err)
				return
			}
		}

		storeTask := (*store.Task[any, int])(task)
		err := handler(storeTask)

		if err == nil {
			log.Printf("{GoroutineID: %d} Marking task %d (current status: %s) as DONE", 
				shared.GoroutineID(), task.ID, task.Status)
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

func (ts *InMemoryTaskScheduler) FindTasksInStatus(ctx context.Context, task_status store.TaskStatus) ([]store.Task[any, int], error) {
	tasks, err := ts.store.FindTasksInStatus(ctx, task_status)
	if err != nil {
		return nil, err
	}

	converted_tasks := make([]store.Task[any, int], len(tasks))
	for i, task := range tasks {
		converted_tasks[i] = store.Task[any, int](task)
	}
	return converted_tasks, nil
}