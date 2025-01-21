package leader_election

import (
	"context"
)

type LeaderElection interface {
	IsLeader(ctx context.Context) (bool, error)
	Start(ctx context.Context) error
	Stop() error
	ElectLeader(ctx context.Context) (bool, error)
	Resign() error
}
