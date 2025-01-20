package scheduler

import "context"

// LeaderElection defines the interface for leader election implementations
type LeaderElection interface {
	// ElectLeader attempts to become the leader
	ElectLeader(ctx context.Context) (bool, error)

	// IsLeader checks if the current instance is the leader
	IsLeader(ctx context.Context) (bool, error)

	// Resign relinquishes leadership if currently the leader
	Resign(ctx context.Context) error
}
