package inmemory

import (
	"context"
	"errors"
	"fmt"
	"testing"

	inmemory_scheduler "mongopher-scheduler/task_scheduler/scheduler/inmemory"
	"mongopher-scheduler/task_scheduler/store"
	inmemory_store "mongopher-scheduler/task_scheduler/store/inmemory"

	"github.com/stretchr/testify/assert"
)

func TestFailingTask(t *testing.T) {
	// Create in-memory store
	_ = inmemory_store.NewInMemoryStore()

	// Create in-memory scheduler
	scheduler := inmemory_scheduler.NewInMemoryTaskScheduler()
	scheduler.StartScheduler(context.Background())

	// Create a failing task handler
	handler := func(task *store.Task[any, int]) error {
		return errors.New("task failed")
	}

	// Register the handler
	scheduler.RegisterHandler("failing-task", inmemory_scheduler.InMemoryTaskHandler(handler))

	// Register and schedule the task
	_, err := scheduler.RegisterTask("failing-task", inmemory_scheduler.NewAnyStructParameter(0), nil)
	assert.NoError(t, err)

	// Start scheduler
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scheduler.StartScheduler(ctx)

	// Wait for task to complete
	done, err := scheduler.WaitForAllTasksToBeInStatus(store.StatusException)
	assert.NoError(t, err)
	assert.True(t, done)

	// Verify task status is failed
	tasks, err := scheduler.FindTasksInStatus(ctx, store.StatusException)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tasks))
}

func TestMultipleFailingTasks(t *testing.T) {
	// Create in-memory store
	_ = inmemory_store.NewInMemoryStore()

	// Create in-memory scheduler
	scheduler := inmemory_scheduler.NewInMemoryTaskScheduler()
	scheduler.StartScheduler(context.Background())

	// Create a failing task handler
	handler := func(task *store.Task[any, int]) error {
		return errors.New("task failed")
	}

	// Register the handler
	scheduler.RegisterHandler("failing-task", inmemory_scheduler.InMemoryTaskHandler(handler))

	// Create and register 10 failing tasks
	for i := 0; i < 10; i++ {
		taskName := fmt.Sprintf("failing-task-%d", i)
		_, err := scheduler.RegisterTask(taskName, inmemory_scheduler.NewAnyStructParameter(i), nil)
		assert.NoError(t, err)
	}

	// Start scheduler
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scheduler.StartScheduler(ctx)

	// Wait for tasks to complete
	done, err := scheduler.WaitForAllTasksToBeInStatus(store.StatusException)
	assert.NoError(t, err)
	assert.True(t, done)

	// Verify all tasks failed
	tasks, err := scheduler.FindTasksInStatus(ctx, store.StatusException)
	assert.NoError(t, err)
	assert.Equal(t, 10, len(tasks))
}
