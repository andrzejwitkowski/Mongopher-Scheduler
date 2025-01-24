package leader_election

import (
	"context"
	"sync"
	"testing"

	"github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/shared"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/stretchr/testify/assert"
)

func TestMongoLeaderElection_HappyPath(t *testing.T) {
	connStr, cleanup := shared.SetupMongoDB(t)
	defer cleanup("testdb")

	// Create MongoDB client
	clientOptions := options.Client().ApplyURI(connStr)
	client, err := mongo.Connect(context.Background(), clientOptions)
	assert.NoError(t, err)

	le1, _ := NewDefaultMongoLeaderElection("instance-1", client, "testdb")

	// Become leader
	isLeader, err := le1.ElectLeader(context.Background())
	assert.NoError(t, err)
	assert.True(t, isLeader)

	// Verify leadership status
	isLeader, err = le1.IsLeader(context.Background())
	assert.NoError(t, err)
	assert.True(t, isLeader)

	// Verify still leader
	isLeader, err = le1.IsLeader(context.Background())
	assert.NoError(t, err)
	assert.True(t, isLeader)

	// Gracefully resign
	err = le1.Resign()
	assert.NoError(t, err)

	// Verify no longer leader
	isLeader, err = le1.IsLeader(context.Background())
	assert.NoError(t, err)
	assert.False(t, isLeader)
}

func TestMongoLeaderElection_Failover(t *testing.T) {
	connStr, cleanup := shared.SetupMongoDB(t)
	defer cleanup("testdb")

	// Create MongoDB client
	clientOptions := options.Client().ApplyURI(connStr)
	client, err := mongo.Connect(context.Background(), clientOptions)
	assert.NoError(t, err)

	le1, _ := NewDefaultMongoLeaderElection("instance-1", client, "testdb")
	isLeader, err := le1.ElectLeader(context.Background())
	assert.NoError(t, err)
	assert.True(t, isLeader)

	le2, _ := NewDefaultMongoLeaderElection("instance-2", client, "testdb")
	
	le1.Resign()

	// Second instance should now become leader
	isLe2Leader, err := le2.ElectLeader(context.Background())
	assert.NoError(t, err)
	assert.True(t, isLe2Leader)

	// Verify first instance is no longer leader
	isLe1Leader, err := le1.IsLeader(context.Background())
	assert.NoError(t, err)
	assert.False(t, isLe1Leader)
}

func TestMongoLeaderElection_Concurrent(t *testing.T) {
	connStr , cleanup := shared.SetupMongoDB(t)
	defer cleanup("testdb")

	// Create MongoDB client
	clientOptions := options.Client().ApplyURI(connStr)
	client, err := mongo.Connect(context.Background(), clientOptions)
	assert.NoError(t, err)

	le1, _ := NewDefaultMongoLeaderElection("instance-1", client, "testdb")
	le2, _ := NewDefaultMongoLeaderElection("instance-2", client, "testdb")

	// Use a barrier to ensure both elections start at the same time
	var startBarrier, endBarrier sync.WaitGroup
	startBarrier.Add(2)
	endBarrier.Add(2)

	var le1Result, le2Result bool
	var le1Err, le2Err error

	go func() {
		startBarrier.Done()
		startBarrier.Wait() // Wait for both goroutines to be ready
		le1Result, le1Err = le1.ElectLeader(context.Background())
		endBarrier.Done()
	}()

	go func() {
		startBarrier.Done()
		startBarrier.Wait() // Wait for both goroutines to be ready
		le2Result, le2Err = le2.ElectLeader(context.Background())
		endBarrier.Done()
	}()

	endBarrier.Wait()

	// Only one should be leader
	leaderCount := 0
	if le1Result {
		leaderCount++
	}
	if le2Result {
		leaderCount++
	}
	assert.Equal(t, 1, leaderCount, "Only one instance should be leader")

	// Verify errors
	assert.NoError(t, le1Err)
	assert.NoError(t, le2Err)

	// Verify consistent state
	if le1Result {
		isLeader, err := le1.IsLeader(context.Background())
		assert.NoError(t, err)
		assert.True(t, isLeader)

		isLeader, err = le2.IsLeader(context.Background())
		assert.NoError(t, err)
		assert.False(t, isLeader)
	} else {
		isLeader, err := le2.IsLeader(context.Background())
		assert.NoError(t, err)
		assert.True(t, isLeader)

		isLeader, err = le1.IsLeader(context.Background())
		assert.NoError(t, err)
		assert.False(t, isLeader)
	}
}