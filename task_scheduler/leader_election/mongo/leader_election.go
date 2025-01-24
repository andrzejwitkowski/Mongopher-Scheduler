package leader_election

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/shared"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)


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

type LeaderInfo struct {
	InstanceID string `bson:"instance_id"`
	LastSeen   time.Time `bson:"last_seen"`
}

func NewDefaultMongoLeaderElection(instanceID string, client *mongo.Client, database string) (*MongoLeaderElection, error) {
	return NewMongoLeaderElection(instanceID, client, database, 30*time.Second)
}

func NewMongoLeaderElection(instanceID string, client *mongo.Client, database string, leaderTTL time.Duration) (*MongoLeaderElection, error) {
	if leaderTTL <= 0 {
		return nil, fmt.Errorf("leaderTTL must be greater than 0")
	}
	if instanceID == "" {
		return nil, fmt.Errorf("instanceID cannot be empty")
	}
	if client == nil {
		return nil, fmt.Errorf("client cannot be nil")
	}
	if database == "" {
		return nil, fmt.Errorf("database cannot be empty")
	}

	mle := &MongoLeaderElection{
		instanceID:   instanceID,
		client:       client,
		database:     database,
		collection:   "leader_election",
		timeProvider: shared.DefaultTimeProvider(),
		refreshDone:  make(chan struct{}),
		leaderTTL:    leaderTTL,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	if err := mle.ensureIndexes(ctx); err != nil {
		return nil, fmt.Errorf("failed to ensure indexes: %w", err)
	}
	
	return mle, nil
}

func (mle *MongoLeaderElection) ElectLeader(ctx context.Context) (bool, error) {
    mle.mu.Lock()
    defer mle.mu.Unlock()

    collection := mle.client.Database(mle.database).Collection(mle.collection)
    now := mle.timeProvider.Now()
    ttlThreshold := now.Add(-mle.leaderTTL)

    // First, try to insert a single leader document if it doesn't exist
    _, err := collection.InsertOne(ctx, bson.M{
        "leader_key": "singleton",  // Fixed key for the single leader document
        "instance_id": mle.instanceID,
        "last_seen": now,
    })

    if err != nil {
        // If document already exists, try to take over leadership
        if mongo.IsDuplicateKeyError(err) {
        	// First check if current leader is expired
        	currentLeader, err := mle.getCurrentLeader(ctx)
        	if err != nil && err != mongo.ErrNoDocuments {
        		return false, err
        	}
       
        	// If there's no current leader or it's expired, try to take over
        	if err == mongo.ErrNoDocuments || time.Since(currentLeader.LastSeen) >= mle.leaderTTL {
        		filter := bson.M{
        			"leader_key": "singleton",
        			"last_seen": bson.M{"$lt": ttlThreshold},
        		}
       
        		update := bson.M{
        			"$set": bson.M{
        				"instance_id": mle.instanceID,
        				"last_seen":   now,
        			},
        		}
       
        		result, err := collection.UpdateOne(ctx, filter, update)
        		if err != nil {
        			return false, err
        		}
       
        		// If we modified a document, we became the leader
        		if result.ModifiedCount > 0 {
        			mle.isLeader = true
        			ctx, cancel := context.WithCancel(ctx)
        			mle.cancelFunc = cancel
        			go mle.refreshLeadership(ctx)
        			return true, nil
        		}
        	}
       
        	// If we didn't modify any document, someone else is the leader
        	return false, nil
        }
        return false, err
    }

    // If we successfully inserted, we're the leader
    mle.isLeader = true
    return true, nil
}

// Make sure to create a unique index on leader_key
func (mle *MongoLeaderElection) ensureIndexes(ctx context.Context) error {
    collection := mle.client.Database(mle.database).Collection(mle.collection)
    
    _, err := collection.Indexes().CreateOne(ctx, mongo.IndexModel{
        Keys: bson.D{
            {Key: "leader_key", Value: 1},
        },
        Options: options.Index().SetUnique(true),
    })
    if err != nil {
        return err
    }

    // Create TTL index on last_seen field
    _, err = collection.Indexes().CreateOne(ctx, mongo.IndexModel{
        Keys: bson.D{
            {Key: "last_seen", Value: 1},
        },
        Options: options.Index().
            SetExpireAfterSeconds(int32(mle.leaderTTL.Seconds())).
            SetBackground(true),
    })
    return err
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

	// If we can't find the leader document, we're not the leader
	if err == mongo.ErrNoDocuments {
		mle.isLeader = false
		return false, nil
	}

	// If we found a leader document but it's expired, we're not the leader
	if time.Since(leader.LastSeen) >= mle.leaderTTL {
		mle.isLeader = false
		return false, nil
	}

	// If we're the leader in the document, return true
	if leader.InstanceID == mle.instanceID {
		return true, nil
	}

	// Someone else is the leader
	mle.isLeader = false
	return false, nil
}

func (le *MongoLeaderElection) Start(ctx context.Context) error {
	// Start refresh goroutine
	ctx, cancel := context.WithCancel(ctx)
	le.cancelFunc = cancel
	go le.refreshLeadership(ctx)
	return nil
}

func (le *MongoLeaderElection) Stop() error {
	if le.cancelFunc != nil {
		le.cancelFunc()
	}
	return nil
}

func (mle *MongoLeaderElection) Resign() error {
	mle.mu.Lock()
	defer mle.mu.Unlock()

	if mle.isLeader {
		collection := mle.client.Database(mle.database).Collection(mle.collection)
		filter := bson.M{"instance_id": mle.instanceID}
		_, err := collection.DeleteOne(context.Background(), filter)
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

func (mle *MongoLeaderElection) getCurrentLeader(ctx context.Context) (*LeaderInfo, error) {
    collection := mle.client.Database(mle.database).Collection(mle.collection)
    
    var leader LeaderInfo
    filter := bson.M{"leader_key": "singleton"}
    err := collection.FindOne(ctx, filter).Decode(&leader)
    if err != nil {
        return nil, err
    }
    return &leader, nil
}

func (mle *MongoLeaderElection) updateLeaderInfo(ctx context.Context) error {
    collection := mle.client.Database(mle.database).Collection(mle.collection)
    now := mle.timeProvider.Now()

    filter := bson.M{
        "leader_key": "singleton",
        "instance_id": mle.instanceID,
    }

    update := bson.M{
        "$set": bson.M{
            "last_seen": now,
        },
    }

    result, err := collection.UpdateOne(ctx, filter, update)
    if err != nil {
        return err
    }

    if result.ModifiedCount == 0 {
        mle.isLeader = false
        if mle.cancelFunc != nil {
            mle.cancelFunc()
        }
    }

    return nil
}
