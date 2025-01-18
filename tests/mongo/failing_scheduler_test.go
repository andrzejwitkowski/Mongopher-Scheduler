package mongo

import (
	"context"
	"errors"
	"fmt"
	"testing"

	mongo_scheduler "mongopher-scheduler/task_scheduler/scheduler/mongo"
	"mongopher-scheduler/task_scheduler/store"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestFailingTask(t *testing.T) {
	connStr, cleanup := setupMongoDB(t)
	defer cleanup("testdb")

	// Create MongoDB client
	clientOptions := options.Client().ApplyURI(connStr)
	client, err := mongo.Connect(context.Background(), clientOptions)
	assert.NoError(t, err)

	// Create MongoDB scheduler
	scheduler := mongo_scheduler.NewMongoTaskScheduler(client, "testdb")
	scheduler.StartScheduler(context.Background())
	defer scheduler.StopScheduler()

	// Create a failing task handler
	handler := func(task *store.Task[bson.M, primitive.ObjectID]) error {
		return errors.New("task failed")
	}

	// Register the handler
	scheduler.RegisterHandler("failing-task", handler)

	// Register and schedule the task
	_, err = scheduler.RegisterTask("failing-task", mongo_scheduler.MongoTaskParameter{"value": 0}, nil)
	assert.NoError(t, err)

	// Start scheduler
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	connStr, cleanup := setupMongoDB(t)
	defer cleanup("testdb")

	// Create MongoDB client
	clientOptions := options.Client().ApplyURI(connStr)
	client, err := mongo.Connect(context.Background(), clientOptions)
	assert.NoError(t, err)

	// Create MongoDB scheduler
	scheduler := mongo_scheduler.NewMongoTaskScheduler(client, "testdb")
	scheduler.StartScheduler(context.Background())
	defer scheduler.StopScheduler()

	// Create a failing task handler
	handler := func(task *store.Task[bson.M, primitive.ObjectID]) error {
		return errors.New("task failed")
	}
	// Create and register 10 failing tasks
	for i := 0; i < 10; i++ {
		taskName := fmt.Sprintf("failing-task-%d", i)
		scheduler.RegisterHandler(taskName, mongo_scheduler.MongoTaskHandler(handler))
		_, err := scheduler.RegisterTask(taskName, mongo_scheduler.MongoTaskParameter{"value": i}, nil)
		assert.NoError(t, err)
	}

	// Start scheduler
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Wait for tasks to complete
	done, err := scheduler.WaitForAllTasksToBeInStatus(store.StatusException)
	assert.NoError(t, err)
	assert.True(t, done)

	// Verify all tasks failed
	tasks, err := scheduler.FindTasksInStatus(ctx, store.StatusException)
	assert.NoError(t, err)
	assert.Equal(t, 10, len(tasks))
}
