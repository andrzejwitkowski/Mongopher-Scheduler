package inmemory

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"mongopher-scheduler/task_scheduler/scheduler/inmemory"
	"mongopher-scheduler/task_scheduler/store"
)

func TestSingleTaskSuccess(t *testing.T) {
	// Create new in-memory scheduler
	scheduler := inmemory.NewInMemoryTaskScheduler()
	scheduler.StartScheduler(context.Background())

	// Create a simple task handler that always succeeds
	handler := func(task *store.Task[any, int]) error {
		return nil
	}

	// Create task with the handler
	scheduler.RegisterHandler("test-task-1", handler)

	// Register and schedule the task
	_, err := scheduler.RegisterTask("test-task-1", inmemory.NewAnyStructParameter(0), nil)
	assert.NoError(t, err)

	// Start scheduler
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scheduler.StartScheduler(ctx)

	// Wait for task to complete using observer
	done, err := scheduler.WaitForAllTasksToBeDone()
	assert.NoError(t, err)
	assert.True(t, done)

	// Verify task status
	tasks, err := scheduler.FindTasksInStatus(ctx, store.StatusDone)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tasks))
}

func TestMultipleTasksSuccess(t *testing.T) {
	// Create new in-memory scheduler
	scheduler := inmemory.NewInMemoryTaskScheduler()
	scheduler.StartScheduler(context.Background())

	// Create a simple task handler that always succeeds
	handler := func(task *store.Task[any, int]) error {
		return nil
	}

	// Start scheduler
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scheduler.StartScheduler(ctx)

	// Create and register 10 tasks
	for i := 0; i < 10; i++ {
		taskName := fmt.Sprintf("test-task-%d", i)
		scheduler.RegisterHandler(taskName, handler)
		_, err := scheduler.RegisterTask(taskName, inmemory.NewAnyStructParameter(i), nil)
		assert.NoError(t, err)
	}

	// Wait for tasks to complete using observer
	done, err := scheduler.WaitForAllTasksToBeDone()
	assert.NoError(t, err)
	assert.True(t, done)

	// Verify all tasks completed successfully
	tasks, err := scheduler.FindTasksInStatus(ctx, store.StatusDone)
	assert.NoError(t, err)
	assert.Equal(t, 10, len(tasks))
}

func TestFailingTaskWithRetries(t *testing.T) {
	// Create new in-memory scheduler
	scheduler := inmemory.NewInMemoryTaskScheduler()
	scheduler.StartScheduler(context.Background())

	// Create a task handler that fails 4 times before succeeding
	handler := func(task *store.Task[any, int]) error {
		if task.RetryConfig.Attempts < 5 {
			return fmt.Errorf("simulated failure attempt %d", task.RetryConfig.Attempts + 1)
		}
		return nil
	}

	// Register the handler and task
	taskName := "failing-task"
	scheduler.RegisterHandler(taskName, handler)
	_, err := scheduler.RegisterTask(taskName, inmemory.NewAnyStructParameter(0), nil)
	assert.NoError(t, err)

	// Start scheduler
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scheduler.StartScheduler(ctx)

	// Wait for task to complete using observer with retry options
	done, err := scheduler.WaitForAllTasksToBeDone()
	assert.NoError(t, err)
	assert.True(t, done)

	// Verify task completed successfully after retries
	tasks, err := scheduler.FindTasksInStatus(ctx, store.StatusDone)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tasks))

	// Verify the task history shows the retries
	task := tasks[0]
	assert.Equal(t, 5, task.RetryConfig.Attempts)
}
