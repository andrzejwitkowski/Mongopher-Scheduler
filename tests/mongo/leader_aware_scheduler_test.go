package mongo

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	mongo_leader_election "github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/leader_election/mongo"
	leader_scheduler "github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/scheduler"
	mongo_scheduler "github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/scheduler/mongo"
	"github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/shared"
	"github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/store"
)

// Helper function from existing test patterns
func createMongoClient(t *testing.T, connStr string) *mongo.Client {
	clientOptions := options.Client().ApplyURI(connStr)
	client, err := mongo.Connect(context.Background(), clientOptions)
	assert.NoError(t, err)
	return client
}

func TestLeaderAwareTaskScheduler_HappyPath(t *testing.T) {

	connStr, cleanup := shared.SetupMongoDB(t)
	defer cleanup("testdb")
	
	// Create shared client
	client := createMongoClient(t, connStr)
	
	// Create first leader-aware scheduler
	leaderElection1 := shared.Must(mongo_leader_election.NewDefaultMongoLeaderElection("instance-1", client, "testdb"))
	scheduler1 := mongo_scheduler.NewMongoTaskScheduler(client, "testdb")
	las := leader_scheduler.NewLeaderAwareTaskScheduler(scheduler1, leaderElection1)

	// Register hander
	las.RegisterHandler("test-task", func(task leader_scheduler.Task) error {
		log.Printf("Task: %v", task)
		time.Sleep(time.Second * 2)
		return nil
	})

	// Start scheduler
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	las.StartScheduler(ctx)

	// Test leader election
	tasks, err := las.FindTasksInStatus(context.Background(), store.StatusNew)
	assert.NoError(t, err)
	assert.Empty(t, tasks)

	// Register a task
	task, err := las.RegisterTask(ctx, "test-task", map[string]interface{}{"param": "value"}, nil)
	assert.NoError(t, err)
	assert.NotNil(t, task)

	// Verify task was registered
	tasks, err = las.FindTasksInStatus(ctx, store.StatusNew)
	assert.NoError(t, err)
	assert.Len(t, tasks, 1)
	assert.Equal(t, "test-task", tasks[0].GetName())
	assert.Equal(t, map[string]interface{}{"param": "value"}, tasks[0].GetParams())

	done, err := las.WaitForAllTasksToBeDone()
	assert.NoError(t, err)
	assert.True(t, done)

	// Test cleanup
	err = las.Close()
	assert.NoError(t, err)
}

func TestLeaderAwareTaskScheduler_LeaderElectionBehavior(t *testing.T) {
	// Create shared task handling tracking
	taskHandled := make(chan string, 1)
	connStr, cleanup := shared.SetupMongoDB(t)
	defer cleanup("testdb")
	
	// Create shared client
	client := createMongoClient(t, connStr)
	
	// Create first leader-aware scheduler
	leaderElection1 := shared.Must(mongo_leader_election.NewDefaultMongoLeaderElection("instance-1", client, "testdb"))
	scheduler1 := mongo_scheduler.NewMongoTaskScheduler(client, "testdb")
	las1 := leader_scheduler.NewLeaderAwareTaskScheduler(scheduler1, leaderElection1)
	
	// Create second scheduler instance
	leaderElection2 := shared.Must(mongo_leader_election.NewDefaultMongoLeaderElection("instance-1", client, "testdb"))
	scheduler2 := mongo_scheduler.NewMongoTaskScheduler(client, "testdb")
	las2 := leader_scheduler.NewLeaderAwareTaskScheduler(scheduler2, leaderElection2)
	
	// Register handler on both schedulers
	handler := func(task leader_scheduler.Task) error {
		taskHandled <- "handled"
		return nil
	}
	las1.RegisterHandler("test-task", handler)
	las2.RegisterHandler("test-task", handler)
	
	// Start both schedulers
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	las1.StartScheduler(ctx)
	las2.StartScheduler(ctx)
	
	// Wait for leader election to settle
	time.Sleep(100 * time.Millisecond)
	
	// Schedule task
	task, err := las1.RegisterTask(ctx, "test-task", nil, nil)
	assert.NoError(t, err)
	assert.NotNil(t, task)
	
	// Wait for task to be handled
	select {
	case <-taskHandled:
		// Task was handled by one of the schedulers
	case <-time.After(1 * time.Second):
		t.Fatal("Task was not handled within timeout")
	}
	
	// Verify only one scheduler handled the task
	select {
	case <-taskHandled:
		t.Fatal("Task was handled by both schedulers")
	case <-time.After(100 * time.Millisecond):
		// Expected - only one handler should have processed the task
	}
	
	// Verify leader status
	leader1, _ := leaderElection1.IsLeader(context.Background())
	leader2, _ := leaderElection2.IsLeader(context.Background())
	
	assert.False(t, leader1 && leader2, "Both schedulers cannot be leaders")
	assert.True(t, leader1 || leader2, "One scheduler must be leader")
	
	// Cleanup
	assert.NoError(t, las1.Close())
	assert.NoError(t, las2.Close())
}