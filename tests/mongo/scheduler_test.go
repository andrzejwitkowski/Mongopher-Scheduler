package mongo

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	mongo_scheduler "mongopher-scheduler/task_scheduler/scheduler/mongo"
	"mongopher-scheduler/task_scheduler/store"
)

var (
	mongoContainer testcontainers.Container
	mongoOnce      sync.Once
)

func setupMongoDB(t *testing.T) (string, func(string)) {
	var connStr string
	var err error
	
	mongoOnce.Do(func() {
		ctx := context.Background()
		
		// Start MongoDB container
		req := testcontainers.ContainerRequest{
			Image:        "mongo:latest",
			ExposedPorts: []string{"27017/tcp"},
			WaitingFor:   wait.ForLog("Waiting for connections"),
		}
		
		mongoContainer, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		assert.NoError(t, err)
	})
		
	
	ctx := context.Background()

	// Get connection string
	host, err := mongoContainer.Host(ctx)
	assert.NoError(t, err)
	
	port, err := mongoContainer.MappedPort(ctx, "27017")
	assert.NoError(t, err)
	
	connStr = fmt.Sprintf("mongodb://%s:%s", host, port.Port())
	

	// Cleanup function to remove all collections
	cleanup := func(database_name string) {
		clientOptions := options.Client().ApplyURI(connStr)
		client, err := mongo.Connect(context.Background(), clientOptions)
		assert.NoError(t, err)

		db := client.Database(database_name)
		collections, err := db.ListCollectionNames(context.Background(), bson.M{})
		assert.NoError(t, err)

		for _, coll := range collections {
			err = db.Collection(coll).Drop(context.Background())
			assert.NoError(t, err)
		}
	}

	return connStr, cleanup
}

func TestSingleTaskSuccess(t *testing.T) {
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

	// Create a simple task handler that always succeeds
	handler := func(task *store.Task[bson.M, primitive.ObjectID]) error {
		return nil
	}

	// Create task with the handler
	scheduler.RegisterHandler("test-task-1", mongo_scheduler.MongoTaskHandler(handler))

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

	// Create a simple task handler that always succeeds
	handler := func(task *store.Task[bson.M, primitive.ObjectID]) error {
		return nil
	}

	for i := 0; i < 10; i++ {
		taskName := fmt.Sprintf("test-task-%d", i)
		scheduler.RegisterHandler(taskName, mongo_scheduler.MongoTaskHandler(handler))
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

	// Create a task handler that fails 4 times before succeeding
	handler := func(task *store.Task[bson.M, primitive.ObjectID]) error {
		if task.RetryConfig.Attempts < 5 {
			return fmt.Errorf("simulated failure attempt %d", task.RetryConfig.Attempts + 1)
		}
		return nil
	}

	// Register the handler and task
	taskName := "failing-task"
	scheduler.RegisterHandler(taskName, mongo_scheduler.MongoTaskHandler(handler))
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
	assert.Equal(t, 5, task.RetryConfig.Attempts)
}
