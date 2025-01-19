package inmemory

import (
	"context"
	"errors"
	"log"
	"github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/store"
	"sync"
	"time"
)

type InMemoryTask store.Task[any, int]

type InMemoryStore struct {
	tasks sync.Map
	mutex sync.Mutex
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		tasks: sync.Map{},
	}
}

func (ms *InMemoryStore) InsertTask(ctx context.Context, task InMemoryTask) (*InMemoryTask, error) {
	ms.tasks.Store(task.ID, &task)
	return &task, nil
}

func (ms *InMemoryStore) GetTaskByID(ctx context.Context, id int) (*InMemoryTask, error) {
	task, ok := ms.tasks.Load(id)
	if !ok {
		return nil, errors.New("task not found")
	}
	return task.(*InMemoryTask), nil
}

func (ms *InMemoryStore) DeleteTask(ctx context.Context, id int) error {
	_, ok := ms.tasks.Load(id)
	if !ok {
		return errors.New("task not found")
	}
	ms.tasks.Delete(id)
	return nil
}

func (ms *InMemoryStore) FindTasksDue(ctx context.Context) ([]InMemoryTask, error) {
	var tasks []InMemoryTask
	for _, task := range ms.tasks.Range {
		task := task.(*InMemoryTask)
		log.Printf("task scheduledAt: %v", task.ScheduledAt)
		if task.Status == store.StatusNew || ( task.Status == store.StatusRetrying && 
			task.ScheduledAt != nil && task.ScheduledAt.Before(time.Now())) {
			tasks = append(tasks, *task)
		}
	}
	return tasks, nil
}

func (ms *InMemoryStore) FindTasksDueAndUpdateToInProgress(ctx context.Context) ([]InMemoryTask, error) {
	tasks, err := ms.FindTasksDue(ctx)
	if err != nil {
		return nil, err
	}
	for _, task := range tasks {
		err := ms.UpdateTaskStatus(ctx, task.ID, store.StatusInProgress, "")
		if err != nil {
			return nil, err
		}
	}
	return tasks, nil
}

func (ms *InMemoryStore) UpdateTaskStatus(ctx context.Context, id int, status store.TaskStatus, errorMsg string) error {
	ms.mutex.Lock()
	defer ms.mutex.Unlock()

	task, ok := ms.tasks.Load(id)
	if !ok {
		return errors.New("task not found")
	}
	task.(*InMemoryTask).Status = status
	ms.tasks.Store(id, task)
	return nil
}

func (ms *InMemoryStore) UpdateTaskState(ctx context.Context, id int, status store.TaskStatus, errorMsg string, retryAttempts int, scheduledAt *time.Time) error {
	ms.mutex.Lock()
	defer ms.mutex.Unlock()

	task, ok := ms.tasks.Load(id)
	if !ok {
		return errors.New("task not found")
	}

	task.(*InMemoryTask).Status = status
	task.(*InMemoryTask).RetryConfig.Attempts = retryAttempts
	if scheduledAt != nil {
		task.(*InMemoryTask).ScheduledAt = scheduledAt
	}
	ms.tasks.Store(id, task)
	return nil
}

func (ms *InMemoryStore) UpdateTaskRetry(ctx context.Context, id int, attempts int, nextScheduledTime *time.Time) error {
	ms.mutex.Lock()
	defer ms.mutex.Unlock()

	task, ok := ms.tasks.Load(id)
	if !ok {
		return errors.New("task not found")
	}

	task.(*InMemoryTask).RetryConfig.Attempts = attempts
	if nextScheduledTime != nil {
		task.(*InMemoryTask).ScheduledAt = nextScheduledTime
	}
	ms.tasks.Store(id, task)
	return nil
}

func (ms *InMemoryStore) AddTaskHistory(ctx context.Context, id int, history store.TaskHistory) error {
	ms.mutex.Lock()
	defer ms.mutex.Unlock()

	task, ok := ms.tasks.Load(id)
	if !ok {
		return errors.New("task not found")
	}
	task.(*InMemoryTask).History = append(task.(*InMemoryTask).History, history)
	ms.tasks.Store(id, task)
	return nil
}

func (ms *InMemoryStore) GetAllTasks(ctx context.Context) ([]InMemoryTask, error) {
	var tasks []InMemoryTask
	for _, task := range ms.tasks.Range {
		task := task.(*InMemoryTask)
		tasks = append(tasks, *task)
	}
	return tasks, nil
}

func (ms *InMemoryStore) FindTasksInStatus(ctx context.Context, task_status store.TaskStatus) ([]InMemoryTask, error) {
	var tasks []InMemoryTask
	for _, task := range ms.tasks.Range {
		task := task.(*InMemoryTask)
		if task.Status == task_status {
			tasks = append(tasks, *task)
		}
	}
	return tasks, nil
}
