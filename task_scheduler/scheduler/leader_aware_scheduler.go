package scheduler

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/store"
)

var ErrNotLeader = errors.New("not the current leader")

type LeaderAwareTaskScheduler struct {
	scheduler      TaskScheduler
	leaderElection LeaderElection
	eventRegistry  *LeaderEventRegistry
	heartbeat      HeartbeatManager
	isLeader       bool
	mu             sync.Mutex
	cancelFunc     context.CancelFunc
}

func NewLeaderAwareTaskScheduler(
	scheduler TaskScheduler,
	leaderElection LeaderElection,
	eventRegistry *LeaderEventRegistry,
) *LeaderAwareTaskScheduler {
	ctx, cancel := context.WithCancel(context.Background())

	las := &LeaderAwareTaskScheduler{
		scheduler:      scheduler,
		leaderElection: leaderElection,
		eventRegistry:  eventRegistry,
		heartbeat:      NewHeartbeatManager(5*time.Second, 30*time.Second),
		cancelFunc:     cancel,
	}

	// Register heartbeat checks
	las.heartbeat.Register(
		func(ctx context.Context) error {
			las.mu.Lock()
			defer las.mu.Unlock()

			if las.isLeader {
				if _, err := las.leaderElection.ElectLeader(ctx); err != nil {
					return err
				}
			}
			return nil
		},
		func(ctx context.Context) {
			las.mu.Lock()
			defer las.mu.Unlock()

			las.isLeader = false
			las.eventRegistry.Notify(ctx, LeaderResigned)
		},
	)

	// Attempt to become leader immediately
	if isLeader, err := leaderElection.ElectLeader(ctx); err == nil && isLeader {
		las.isLeader = true
	} else {
		eventRegistry.Register(las)
	}

	las.heartbeat.Start(ctx)
	return las
}

func (las *LeaderAwareTaskScheduler) HandleLeaderEvent(ctx context.Context, event LeaderEventType) {
	las.mu.Lock()
	defer las.mu.Unlock()

	switch event {
	case LeaderElected:
		if isLeader, err := las.leaderElection.IsLeader(ctx); err == nil && isLeader {
			las.isLeader = true
		}
	case LeaderResigned:
		las.isLeader = false
	}
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

func (las *LeaderAwareTaskScheduler) Close() error {
	las.mu.Lock()
	defer las.mu.Unlock()

	if las.cancelFunc != nil {
		las.cancelFunc()
	}

	las.heartbeat.Stop()

	if las.isLeader {
		if err := las.leaderElection.Resign(context.Background()); err != nil {
			return err
		}
		las.isLeader = false
	}

	las.scheduler.StopScheduler()
	return nil
}
