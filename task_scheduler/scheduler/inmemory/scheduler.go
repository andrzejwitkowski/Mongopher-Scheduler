package inmemory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/logging"
	"github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/scheduler"
	"github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/shared"
	"github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/store"
	"github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/store/inmemory"
)

var logger = logging.GetLogger().Sugar()

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

type InMemoryTaskScheduler struct {
	context    *context.Context
	cancelFunc context.CancelFunc
	store      *inmemory.InMemoryStore
	handlers   map[string]func(scheduler.Task) error
}

func NewInMemoryTaskScheduler() *InMemoryTaskScheduler {
	return &InMemoryTaskScheduler{
		store:    inmemory.NewInMemoryStore(),
		handlers: make(map[string]func(scheduler.Task) error),
	}
}

// TaskStatusObserver implementation
func (ts *InMemoryTaskScheduler) WaitForAllTasksToBeDone() (bool, error) {
	return ts.WaitForAllTasksToBeInStatus(store.StatusDone)
}

func (ts *InMemoryTaskScheduler) WaitForAllTasksToBeDoneWithOptions(options scheduler.WaitForTasksOptions) (bool, error) {
	return ts.WaitForAllTasksToBeInStatusWithOptions(store.StatusDone, options)
}

func (ts *InMemoryTaskScheduler) WaitForAllTasksToBeInStatus(status store.TaskStatus) (bool, error) {
	return ts.WaitForAllTasksToBeInStatusWithOptions(status, scheduler.DefaultWaitForTasksOptions())
}

func (ts *InMemoryTaskScheduler) WaitForAllTasksToBeInStatusWithOptions(status store.TaskStatus, options scheduler.WaitForTasksOptions) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), options.Timeout)
	defer cancel()

	for retries := 0; retries < options.MaxRetries; retries++ {
		tasks, err := ts.FindTasksInStatus(ctx, status)
		if err != nil {
			return false, err
		}

		allTasks, err := ts.store.GetAllTasks(ctx)
		if err != nil {
			return false, err
		}

		if len(tasks) == len(allTasks) {
			return true, nil
		}

		time.Sleep(options.RetryDelay)
	}

	return false, nil
}

// Existing scheduler methods...

// RegisterHandler registers a new task handler
func (ts *InMemoryTaskScheduler) RegisterHandler(name string, handler func(scheduler.Task) error) {
	ts.handlers[name] = handler
}

// StartScheduler begins processing tasks
func (ts *InMemoryTaskScheduler) StartScheduler(ctx context.Context) {
	// Create a new context with a cancel function
	ctx, ts.cancelFunc = context.WithCancel(ctx)
	ts.context = &ctx

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				logger.Debugf("Processing tasks")
				ts.processTasks(ctx)
				time.Sleep(1 * time.Second)
			}
		}
	}()
}

func (ts *InMemoryTaskScheduler) StopScheduler() {
	if ts.cancelFunc != nil {
		ts.cancelFunc()
	}
}

// RegisterTask creates a new task in the database
func (ts *InMemoryTaskScheduler) RegisterTask(name string, params map[string]interface{}, scheduledAt *time.Time) (scheduler.Task, error) {
	task := inmemory.InMemoryTask{
		ID:          GetInMemoryTaskIDProvider().GetNextID(),
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
	return ts.store.InsertTask(context.Background(), task)
}

func (ts *InMemoryTaskScheduler) processTasks(ctx context.Context) {
	// Find all tasks that are ready to be executed
	tasks := shared.Must(ts.store.FindTasksDue(ctx))
	logger.Debugf("Found %d tasks to process", len(tasks))
	for _, task := range tasks {
		logger.Debugf("task Id: %d, status: %s", task.ID, task.Status)
	}

	// Mark all tasks as IN_PROGRESS synchronously
	for _, task := range tasks {
		logger.Debugf("Attempting to mark task %d (current status: %s) as IN_PROGRESS", task.ID, task.Status)
		if err := ts.store.UpdateTaskState(ctx, task.ID, store.StatusInProgress, "", task.RetryConfig.Attempts, nil); err != nil {
			logger.Errorf("Error updating task status: %v", err)
			continue
		}
		logger.Debugf("Marked task %d (current status: %s) as IN_PROGRESS",task.ID, task.Status)
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

	if task.RetryConfig.Attempts < task.RetryConfig.MaxRetries {
		if task.Status != store.StatusInProgress {
			logger.Debugf("Marking task %d (current status: %s) as IN_PROGRESS",task.ID, task.Status)
			if err := ts.store.UpdateTaskState(ctx, task.ID, store.StatusInProgress, "", task.RetryConfig.Attempts, nil); err != nil {
				logger.Errorf("Error marking task as IN_PROGRESS: %v", err)
				return
			}
		}

		err := handler(task)

		if err == nil {
			logger.Debugf("Marking task %d (current status: %s) as DONE", task.ID, task.Status)
			ts.store.UpdateTaskState(ctx, task.ID, store.StatusDone, "", task.RetryConfig.Attempts, nil)
			return
		}

		next_attempt := task.RetryConfig.Attempts + 1
		delay := task.RetryConfig.GetStrategy().NextDelay(task.RetryConfig.Attempts)
		nextExecution := time.Now().Add(delay)

		logger.Debugf("Marking task %d (current status: %s) as RETRYING (attempt %d/%d)", task.ID, task.Status, next_attempt, task.RetryConfig.MaxRetries)
		logger.Debugf("Updating task %d (current status: %s) with next execution time: %s", task.ID, task.Status, nextExecution)

		// Update task state for retry
		if err := ts.store.UpdateTaskState(ctx, task.ID, store.StatusRetrying, err.Error(), next_attempt, &nextExecution); err != nil {
			logger.Errorf("Error updating task state: %v", err)
		}
	} else {
		ts.store.UpdateTaskState(ctx, task.ID, store.StatusException, "Max retries exceeded", task.RetryConfig.Attempts, nil)
	}
}

func (ts *InMemoryTaskScheduler) FindTasksInStatus(ctx context.Context, task_status store.TaskStatus) ([]scheduler.Task, error) {
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
