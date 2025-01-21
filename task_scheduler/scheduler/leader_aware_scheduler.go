package scheduler

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/heartbeat"
	"github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/leader_election"
	"github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/shared"
	"github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/store"
)

var ErrNotLeader = errors.New("not the current leader")

type LeaderAwareTaskScheduler struct {
	scheduler      TaskScheduler
	leaderElection leader_election.LeaderElection
	heartbeat      heartbeat.HeartbeatManager
	isLeader       bool
	mu             sync.Mutex
	cancelFunc     context.CancelFunc
}

func NewLeaderAwareTaskScheduler(
	scheduler TaskScheduler,
	leaderElection leader_election.LeaderElection,
) *LeaderAwareTaskScheduler {
	ctx, cancel := context.WithCancel(context.Background())

	las := &LeaderAwareTaskScheduler{
		scheduler:      scheduler,
		leaderElection: leaderElection,
		heartbeat:      heartbeat.NewHeartbeatManager(5*time.Second, 30*time.Second),
		cancelFunc:     cancel,
	}

	// Register heartbeat checks
	las.heartbeat.Register(
		func(ctx context.Context) error {
			las.mu.Lock()
			defer las.mu.Unlock()

			if las.isLeader {
				shared.Must(las.leaderElection.ElectLeader(ctx))
			}
			return nil
		},
		func(ctx context.Context) {
			las.mu.Lock()
			defer las.mu.Unlock()
			las.isLeader = false
		},
	)

	// Attempt to become leader immediately
	if isLeader, err := leaderElection.ElectLeader(ctx); err == nil && isLeader {
		las.isLeader = true
	}

	las.heartbeat.Start(ctx)
	return las
}

func (las *LeaderAwareTaskScheduler) RegisterHandler(name string, handler func(Task) error) {
	las.scheduler.RegisterHandler(name, handler)
}

func (las *LeaderAwareTaskScheduler) RegisterTask(ctx context.Context, name string, params map[string]interface{}, scheduledAt *time.Time) (Task, error) {
	las.mu.Lock()
	defer las.mu.Unlock()

	if !las.isLeader {
		return nil, ErrNotLeader
	}
	return las.scheduler.RegisterTask(name, params, scheduledAt)
}

func (las *LeaderAwareTaskScheduler) FindTasksInStatus(ctx context.Context, status store.TaskStatus) ([]Task, error) {
	las.mu.Lock()
	defer las.mu.Unlock()

	if !las.isLeader {
		return nil, ErrNotLeader
	}
	return las.scheduler.FindTasksInStatus(ctx, status)
}

func (las *LeaderAwareTaskScheduler) StartScheduler(ctx context.Context) {
	las.scheduler.StartScheduler(ctx)
}

func (las *LeaderAwareTaskScheduler) Close() error {
	las.mu.Lock()
	defer las.mu.Unlock()

	if las.cancelFunc != nil {
		las.cancelFunc()
	}

	las.heartbeat.Stop()

	if las.isLeader {
		if err := las.leaderElection.Resign(); err != nil {
			return err
		}
		las.isLeader = false
	}

	las.scheduler.StopScheduler()
	return nil
}

func (las *LeaderAwareTaskScheduler) WaitForAllTasksToBeDone() (bool, error) {
	return las.scheduler.WaitForAllTasksToBeDone()
}

func (las *LeaderAwareTaskScheduler) WaitForAllTasksToBeDoneWithOptions(options WaitForTasksOptions) (bool, error) {
	return las.scheduler.WaitForAllTasksToBeDoneWithOptions(options)
}

func (las *LeaderAwareTaskScheduler) WaitForAllTasksToBeInStatus(status store.TaskStatus) (bool, error) {
	return las.scheduler.WaitForAllTasksToBeInStatus(status)
}

func (las *LeaderAwareTaskScheduler) WaitForAllTasksToBeInStatusWithOptions(status store.TaskStatus, options WaitForTasksOptions) (bool, error) {
	return las.scheduler.WaitForAllTasksToBeInStatusWithOptions(status, options)
}
