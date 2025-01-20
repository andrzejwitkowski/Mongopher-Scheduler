package leader

import (
	"context"
	"log"
	"sync"
	"time"
)

type LeaderAwareScheduler struct {
	leaderElection LeaderElection
	scheduler      TaskScheduler
	electionCtx    context.Context
	cancelElection context.CancelFunc
	mu             sync.Mutex
}

func NewLeaderAwareScheduler(leaderElection LeaderElection, scheduler TaskScheduler) *LeaderAwareScheduler {
	return &LeaderAwareScheduler{
		leaderElection: leaderElection,
		scheduler:      scheduler,
	}
}

func (ls *LeaderAwareScheduler) StartScheduler(ctx context.Context) error {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	ls.electionCtx, ls.cancelElection = context.WithCancel(ctx)
	go ls.monitorLeadership(ls.electionCtx)
	return nil
}

func (ls *LeaderAwareScheduler) monitorLeadership(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// Initial leadership check
	ls.handleLeadership(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ls.handleLeadership(ctx)
		}
	}
}

func (ls *LeaderAwareScheduler) handleLeadership(ctx context.Context) {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	// Check if we're already the leader
	isLeader, err := ls.leaderElection.IsLeader(ctx)
	if err != nil {
		log.Printf("Error checking leadership: %v", err)
		return
	}

	if isLeader {
		if !ls.scheduler.IsRunning() {
			if err := ls.scheduler.StartScheduler(ctx); err != nil {
				log.Printf("Error starting scheduler: %v", err)
			}
		}
		return
	}

	// Try to become leader
	elected, err := ls.leaderElection.ElectLeader(ctx)
	if err != nil {
		log.Printf("Error during leader election: %v", err)
		return
	}

	if elected {
		log.Println("Elected as leader, starting scheduler")
		if err := ls.scheduler.StartScheduler(ctx); err != nil {
			log.Printf("Error starting scheduler: %v", err)
		}
	} else {
		// If not elected, ensure scheduler is stopped
		if ls.scheduler.IsRunning() {
			if err := ls.scheduler.StopScheduler(); err != nil {
				log.Printf("Error stopping scheduler: %v", err)
			}
		}
	}
}

func (ls *LeaderAwareScheduler) StopScheduler() error {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	if ls.cancelElection != nil {
		ls.cancelElection()
	}

	if err := ls.leaderElection.Resign(context.Background()); err != nil {
		return err
	}

	return ls.scheduler.StopScheduler()
}
