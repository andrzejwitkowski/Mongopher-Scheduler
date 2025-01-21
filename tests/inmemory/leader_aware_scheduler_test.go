package inmemory

import (
	"context"
	"log"
	"testing"
	"time"

	inmemory_leader_election "github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/leader_election/inmemory"
	leader_scheduler "github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/scheduler"
	inmemory_scheduler "github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/scheduler/inmemory"
	"github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/store"
	"github.com/stretchr/testify/assert"
)

func TestLeaderAwareTaskScheduler_HappyPath(t *testing.T) {

	// Create in-memory scheduler
	scheduler := inmemory_scheduler.NewInMemoryTaskScheduler()

	// Create in-memory leader election
	leaderElection := inmemory_leader_election.NewLeaderElection("test-instance")

	// Create leader-aware scheduler
	las := leader_scheduler.NewLeaderAwareTaskScheduler(scheduler, leaderElection)

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
