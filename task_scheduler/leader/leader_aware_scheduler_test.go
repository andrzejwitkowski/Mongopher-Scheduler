package leader_test

import (
	"context"
	"testing"
	"time"

	"github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/leader"
	"github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/scheduler/inmemory"
)

func TestLeaderAwareScheduler(t *testing.T) {
	// Reset leader store before test
	leader.Reset()

	// Create two instances to simulate leader election
	leaderElection1 := inmemory.NewLeaderElection("instance-1")
	leaderElection2 := inmemory.NewLeaderElection("instance-2")

	scheduler1 := inmemory.NewInMemoryTaskScheduler()
	scheduler2 := inmemory.NewInMemoryTaskScheduler()

	leaderAwareScheduler1 := leader.NewLeaderAwareScheduler(leaderElection1, scheduler1)
	leaderAwareScheduler2 := leader.NewLeaderAwareScheduler(leaderElection2, scheduler2)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Start both schedulers
	if err := leaderAwareScheduler1.StartScheduler(ctx); err != nil {
		t.Fatalf("Failed to start scheduler 1: %v", err)
	}
	if err := leaderAwareScheduler2.StartScheduler(ctx); err != nil {
		t.Fatalf("Failed to start scheduler 2: %v", err)
	}

	// Wait for leader election
	time.Sleep(2 * time.Second)

	// Verify only one instance is leader
	isLeader1, err := leaderElection1.IsLeader(ctx)
	if err != nil {
		t.Fatalf("Error checking leadership for instance 1: %v", err)
	}

	isLeader2, err := leaderElection2.IsLeader(ctx)
	if err != nil {
		t.Fatalf("Error checking leadership for instance 2: %v", err)
	}

	if isLeader1 && isLeader2 {
		t.Fatal("Both instances became leaders")
	}
	if !isLeader1 && !isLeader2 {
		t.Fatal("No leader was elected")
	}

	// Test leader failover
	if isLeader1 {
		if err := leaderAwareScheduler1.StopScheduler(); err != nil {
			t.Fatalf("Failed to stop scheduler 1: %v", err)
		}
	} else {
		if err := leaderAwareScheduler2.StopScheduler(); err != nil {
			t.Fatalf("Failed to stop scheduler 2: %v", err)
		}
	}

	// Wait for new leader election
	time.Sleep(5 * time.Second)

	// Verify new leader was elected
	isLeader1, err = leaderElection1.IsLeader(ctx)
	if err != nil {
		t.Fatalf("Error checking leadership for instance 1: %v", err)
	}

	isLeader2, err = leaderElection2.IsLeader(ctx)
	if err != nil {
		t.Fatalf("Error checking leadership for instance 2: %v", err)
	}

	if isLeader1 && isLeader2 {
		t.Fatal("Both instances became leaders after failover")
	}
	if !isLeader1 && !isLeader2 {
		t.Fatal("No leader was elected after failover")
	}

	// Cleanup
	if err := leaderAwareScheduler1.StopScheduler(); err != nil {
		t.Errorf("Failed to stop scheduler 1: %v", err)
	}
	if err := leaderAwareScheduler2.StopScheduler(); err != nil {
		t.Errorf("Failed to stop scheduler 2: %v", err)
	}
}
