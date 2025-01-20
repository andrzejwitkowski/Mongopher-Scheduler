package mongo

import (
	"context"
	"sync"
	"time"

	"github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/scheduler"
	"github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/shared"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var _ scheduler.LeaderElection = (*MongoLeaderElection)(nil)

type MongoLeaderElection struct {
	instanceID   string
	client       *mongo.Client
	database     string
	collection   string
	timeProvider shared.TimeProvider
	isLeader     bool
	cancelFunc   context.CancelFunc
	refreshDone  chan struct{}
	mu           sync.Mutex
	leaderTTL    time.Duration
}

func NewMongoLeaderElection(instanceID string, client *mongo.Client, database string) *MongoLeaderElection {
	return &MongoLeaderElection{
		instanceID:   instanceID,
		client:       client,
		database:     database,
		collection:   "leader_election",
		timeProvider: shared.DefaultTimeProvider(),
		refreshDone:  make(chan struct{}),
		leaderTTL:    30 * time.Second,
	}
}

func (mle *MongoLeaderElection) ElectLeader(ctx context.Context) (bool, error) {
	mle.mu.Lock()
	defer mle.mu.Unlock()

	collection := mle.client.Database(mle.database).Collection(mle.collection)

	// Check current leader
	var leader struct {
		InstanceID string    `bson:"instance_id"`
		LastSeen   time.Time `bson:"last_seen"`
	}

	filter := bson.M{}
	err := collection.FindOne(ctx, filter).Decode(&leader)
	if err == nil {
		if time.Since(leader.LastSeen) < mle.leaderTTL && leader.InstanceID != mle.instanceID {
			return false, nil
		}
	}

	// Become leader
	update := bson.M{
		"$set": bson.M{
			"instance_id": mle.instanceID,
			"last_seen":   mle.timeProvider.Now(),
		},
	}
	_, err = collection.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))
	if err != nil {
		return false, err
	}

	mle.isLeader = true

	// Start refresh goroutine
	ctx, cancel := context.WithCancel(ctx)
	mle.cancelFunc = cancel
	go mle.refreshLeadership(ctx)

	return true, nil
}

func (mle *MongoLeaderElection) IsLeader(ctx context.Context) (bool, error) {
	mle.mu.Lock()
	defer mle.mu.Unlock()

	if !mle.isLeader {
		return false, nil
	}

	collection := mle.client.Database(mle.database).Collection(mle.collection)

	var leader struct {
		InstanceID string    `bson:"instance_id"`
		LastSeen   time.Time `bson:"last_seen"`
	}

	filter := bson.M{}
	err := collection.FindOne(ctx, filter).Decode(&leader)
	if err != nil {
		return false, err
	}

	if leader.InstanceID == mle.instanceID && time.Since(leader.LastSeen) < mle.leaderTTL {
		return true, nil
	}

	mle.isLeader = false
	return false, nil
}

func (mle *MongoLeaderElection) Resign(ctx context.Context) error {
	mle.mu.Lock()
	defer mle.mu.Unlock()

	if mle.isLeader {
		collection := mle.client.Database(mle.database).Collection(mle.collection)
		filter := bson.M{"instance_id": mle.instanceID}
		_, err := collection.DeleteOne(ctx, filter)
		if err != nil {
			return err
		}

		mle.isLeader = false
		if mle.cancelFunc != nil {
			mle.cancelFunc()
			<-mle.refreshDone // Wait for refresh to stop
		}
	}
	return nil
}

func (mle *MongoLeaderElection) refreshLeadership(ctx context.Context) {
	defer close(mle.refreshDone)

	ticker := time.NewTicker(mle.leaderTTL / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			mle.mu.Lock()
			if mle.isLeader {
				shared.MustOk(mle.updateLeaderInfo(ctx))
			}
			mle.mu.Unlock()

		case <-ctx.Done():
			return
		}
	}
}

func (mle *MongoLeaderElection) updateLeaderInfo(ctx context.Context) error {
	collection := mle.client.Database(mle.database).Collection(mle.collection)
	filter := bson.M{}
	update := bson.M{
		"$set": bson.M{
			"instance_id": mle.instanceID,
			"last_seen":   mle.timeProvider.Now(),
		},
	}
	_, err := collection.UpdateOne(ctx, filter, update)
	return err
}
