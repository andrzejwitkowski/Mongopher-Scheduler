package inmemory

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/leader"
	"github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/shared"
)

var (
	leaderTTL = 2 * time.Second // Shorter TTL for testing
)

func getLeaderStore() *sync.Map {
	return leader.GetLeaderStore()
}

type leaderInfo struct {
	instanceID string
	lastSeen   time.Time
}

type LeaderElection struct {
	instanceID   string
	isLeader     bool
	cancelFunc   context.CancelFunc
	mu           sync.Mutex
	refreshDone  chan struct{}
	timeProvider shared.TimeProvider
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

	// Clean up any existing refresh goroutine
	if le.cancelFunc != nil {
		le.cancelFunc()
		<-le.refreshDone // Wait for previous refresh to stop
	}

	store := getLeaderStore()
	now := le.timeProvider.Now()

	var elected bool
	// Use atomic store operations
	leader.WithStoreLock(func() {
		// Check current leader status
		if leader, ok := store.Load("leader"); ok {
			leaderInfo := leader.(leaderInfo)
			log.Printf("Checking existing leader: %s, last seen: %v", leaderInfo.instanceID, leaderInfo.lastSeen)
			// If leader is still valid and it's not us, return false
			if now.Sub(leaderInfo.lastSeen) < leaderTTL {
				if leaderInfo.instanceID != le.instanceID {
					log.Printf("Existing leader is still valid: %s", leaderInfo.instanceID)
					elected = false
					return
				}
				// If we're already the leader, return true
				log.Printf("We are already the leader")
				elected = true
				return
			}
			log.Printf("Existing leader has expired")
		}

		// Become leader
		log.Printf("Becoming new leader: %s", le.instanceID)
		store.Store("leader", leaderInfo{
			instanceID: le.instanceID,
			lastSeen:   now,
		})
		le.isLeader = true
		elected = true
	})

	if !elected {
		return false, nil
	}

	// Start refresh goroutine
	ctx, cancel := context.WithCancel(ctx)
	le.cancelFunc = cancel
	le.refreshDone = make(chan struct{}) // Create new channel
	go le.refreshLeadership(ctx)

	return true, nil
}

func (le *LeaderElection) IsLeader(ctx context.Context) (bool, error) {
	le.mu.Lock()
	defer le.mu.Unlock()

	store := getLeaderStore()
	if leader, ok := store.Load("leader"); ok {
		leaderInfo := leader.(leaderInfo)
		now := le.timeProvider.Now()
		log.Printf("Now is: %v, LastSeen: %v - Time since last seen: %v",
			now, leaderInfo.lastSeen, now.Sub(leaderInfo.lastSeen))

		if leaderInfo.instanceID == le.instanceID && now.Sub(leaderInfo.lastSeen) < leaderTTL {
			le.isLeader = true
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
	defer func() {
		if le.refreshDone != nil {
			close(le.refreshDone)
		}
	}()

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
