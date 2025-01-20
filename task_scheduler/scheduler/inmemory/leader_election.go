package inmemory

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/scheduler"
	"github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/shared"
)

var _ scheduler.LeaderElection = (*LeaderElection)(nil)

var (
	leaderStore     *sync.Map
	leaderStoreOnce sync.Once
	leaderTTL       = 30 * time.Second
)

type LeaderElection struct {
	instanceID   string
	isLeader     bool
	cancelFunc   context.CancelFunc
	mu           sync.Mutex
	refreshDone  chan struct{}
	timeProvider shared.TimeProvider
}

func getLeaderStore() *sync.Map {
	leaderStoreOnce.Do(func() {
		leaderStore = &sync.Map{}
	})
	return leaderStore
}

func Reset() {
	leaderStoreOnce = sync.Once{}
	leaderStore = nil
}

func NewLeaderElection(instanceID string) *LeaderElection {
	return &LeaderElection{
		instanceID:   instanceID,
		refreshDone:  make(chan struct{}),
		timeProvider: shared.DefaultTimeProvider(),
	}
}

func NewLeaderElectionWithTimeProvider(instanceID string, timeProvider shared.TimeProvider) *LeaderElection {
	return &LeaderElection{
		instanceID:   instanceID,
		refreshDone:  make(chan struct{}),
		timeProvider: timeProvider,
	}
}

func (le *LeaderElection) ElectLeader(ctx context.Context) (bool, error) {
	le.mu.Lock()
	defer le.mu.Unlock()

	store := getLeaderStore()
	now := le.timeProvider.Now()

	// Check current leader status
	if leader, ok := store.Load("leader"); ok {
		leaderInfo := leader.(leaderInfo)
		if now.Sub(leaderInfo.lastSeen) < leaderTTL && leaderInfo.instanceID != le.instanceID {
			return false, nil
		}
	}

	// Become leader
	store.Store("leader", leaderInfo{
		instanceID: le.instanceID,
		lastSeen:   now,
	})

	le.isLeader = true

	// Start refresh goroutine
	ctx, cancel := context.WithCancel(ctx)
	le.cancelFunc = cancel
	go le.refreshLeadership(ctx)

	return true, nil
}

func (le *LeaderElection) IsLeader(ctx context.Context) (bool, error) {
	le.mu.Lock()
	defer le.mu.Unlock()

	if !le.isLeader {
		return false, nil
	}

	// Verify we're still the leader
	store := getLeaderStore()
	if leader, ok := store.Load("leader"); ok {
		leaderInfo := leader.(leaderInfo)
		log.Printf("Now is: %v, LastSeen: %v - Time since last seen: %v",
			le.timeProvider.Now(), leaderInfo.lastSeen, le.timeProvider.Now().Sub(leaderInfo.lastSeen))
		if leaderInfo.instanceID == le.instanceID && le.timeProvider.Now().Sub(leaderInfo.lastSeen) < leaderTTL {
			return true, nil
		}
	}

	le.isLeader = false
	return false, nil
}

func (le *LeaderElection) Resign(ctx context.Context) error {
	le.mu.Lock()
	defer le.mu.Unlock()

	if le.isLeader {
		store := getLeaderStore()
		store.Delete("leader")
		le.isLeader = false
		if le.cancelFunc != nil {
			le.cancelFunc()
			<-le.refreshDone // Wait for refresh to stop
		}
	}
	return nil
}

func (le *LeaderElection) CancelRefresh() {
	le.mu.Lock()
	defer le.mu.Unlock()

	if le.cancelFunc != nil {
		le.cancelFunc()
		<-le.refreshDone // Wait for refresh to stop
	}
}

func (le *LeaderElection) refreshLeadership(ctx context.Context) {
	defer close(le.refreshDone)

	ticker := time.NewTicker(leaderTTL / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			le.mu.Lock()
			if le.isLeader {
				shared.MustOk(le.updateLeaderInfo())
			}
			le.mu.Unlock()

		case <-ctx.Done():
			return
		}
	}
}

func (le *LeaderElection) updateLeaderInfo() error {
	store := getLeaderStore()
	store.Store("leader", leaderInfo{
		instanceID: le.instanceID,
		lastSeen:   le.timeProvider.Now(),
	})
	return nil
}

type leaderInfo struct {
	instanceID string
	lastSeen   time.Time
}
