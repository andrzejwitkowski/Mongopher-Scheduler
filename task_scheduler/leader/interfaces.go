package leader

import "context"

// LeaderElection defines the interface for leader election
type LeaderElection interface {
	ElectLeader(ctx context.Context) (bool, error)
	IsLeader(ctx context.Context) (bool, error)
	Resign(ctx context.Context) error
}

// TaskScheduler defines the interface for task scheduling
type TaskScheduler interface {
	StartScheduler(ctx context.Context) error
	StopScheduler() error
	IsRunning() bool
}
