package mongo

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	
	mongo_scheduler "github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/scheduler/mongo"
	scheduler_types "github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/scheduler"
	"github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/store"
	"github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/shared"
)

func TestSingleTaskSuccess(t *testing.T) {
	connStr, cleanup := shared.SetupMongoDB(t)
	defer cleanup("testdb")

	// Create MongoDB client
	clientOptions := options.Client().ApplyURI(connStr)
	client, err := mongo.Connect(context.Background(), clientOptions)
	assert.NoError(t, err)

	// Create MongoDB scheduler
	scheduler := mongo_scheduler.NewMongoTaskScheduler(client, "testdb")
	scheduler.StartScheduler(context.Background())
	defer scheduler.StopScheduler()

	// Create a simple task handler that always succeeds
	handler := func(task scheduler_types.Task) error {
		return nil
	}

	// Create task with the handler
	scheduler.RegisterHandler("test-task-1", handler)

	// Register and schedule the task
	_, err = scheduler.RegisterTask("test-task-1", mongo_scheduler.MongoTaskParameter{"value": 0}, nil)
	assert.NoError(t, err)

	// Wait for task to complete using observer
	done, err := scheduler.WaitForAllTasksToBeDone()
	assert.NoError(t, err)
	assert.True(t, done)

	// Verify task status
	tasks, err := scheduler.FindTasksInStatus(context.Background(), store.StatusDone)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tasks))
}

func TestMultipleTasksSuccess(t *testing.T) {
	connStr, cleanup := shared.SetupMongoDB(t)
	defer cleanup("testdb")

	// Create MongoDB client
	clientOptions := options.Client().ApplyURI(connStr)
	client, err := mongo.Connect(context.Background(), clientOptions)
	assert.NoError(t, err)

	// Create MongoDB scheduler
	scheduler := mongo_scheduler.NewMongoTaskScheduler(client, "testdb")
	scheduler.StartScheduler(context.Background())
	defer scheduler.StopScheduler()

	// Create a simple task handler that always succeeds
	handler := func(task scheduler_types.Task) error {
		return nil
	}

	for i := 0; i < 10; i++ {
		taskName := fmt.Sprintf("test-task-%d", i)
		scheduler.RegisterHandler(taskName, handler)
	}

	// Create and register 10 tasks
	for i := 0; i < 10; i++ {
		taskName := fmt.Sprintf("test-task-%d", i)
		_, err := scheduler.RegisterTask(taskName, mongo_scheduler.MongoTaskParameter{"value": i}, nil)
		assert.NoError(t, err)
	}

	// Wait for tasks to complete using observer
	done, err := scheduler.WaitForAllTasksToBeDone()
	assert.NoError(t, err)
	assert.True(t, done)

	// Verify all tasks completed successfully
	tasks, err := scheduler.FindTasksInStatus(context.Background(), store.StatusDone)
	assert.NoError(t, err)
	assert.Equal(t, 10, len(tasks))
}

func TestFailingTaskWithRetries(t *testing.T) {
	connStr, cleanup := shared.SetupMongoDB(t)
	defer cleanup("testdb")

	// Create MongoDB client
	clientOptions := options.Client().ApplyURI(connStr)
	client, err := mongo.Connect(context.Background(), clientOptions)
	assert.NoError(t, err)

	// Create MongoDB scheduler
	scheduler := mongo_scheduler.NewMongoTaskScheduler(client, "testdb")
	scheduler.StartScheduler(context.Background())
	defer scheduler.StopScheduler()

	// Create a task handler that fails 4 times before succeeding
	handler := func(task scheduler_types.Task) error {
		if task.GetRetryConfig().Attempts < 4 {
			return fmt.Errorf("simulated failure attempt %d", task.GetRetryConfig().Attempts+1)
		}
		return nil
	}

	// Register the handler and task
	taskName := "failing-task"
	scheduler.RegisterHandler(taskName, handler)
	_, err = scheduler.RegisterTask(taskName, mongo_scheduler.MongoTaskParameter{"value": 0}, nil)
	assert.NoError(t, err)

	// Wait for task to complete using observer with retry options
	done, err := scheduler.WaitForAllTasksToBeDone()
	assert.NoError(t, err)
	assert.True(t, done)

	// Verify task completed successfully after retries
	tasks, err := scheduler.FindTasksInStatus(context.Background(), store.StatusDone)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tasks))

	// Verify the task history shows the retries
	task := tasks[0]
	assert.Equal(t, 4, task.GetRetryConfig().Attempts)
}
