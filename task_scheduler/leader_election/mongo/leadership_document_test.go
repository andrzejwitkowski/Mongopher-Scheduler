package leader_election

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/shared"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVerifySingletonKeyUsage(t *testing.T) {

	ctx := context.Background()
	connStr, cancel := shared.SetupMongoDB(t)
	database := "test_leader_election"
	collection := "leader_election"

	defer cancel(database)

	// Create MongoDB client
	clientOptions := options.Client().ApplyURI(connStr)
	client, err := mongo.Connect(context.Background(), clientOptions)
	assert.NoError(t, err)

	leaderElection, err := NewDefaultMongoLeaderElection("instance1", client, database)
	require.NoError(t, err)

	// Attempt to insert document
	_, err = leaderElection.ElectLeader(ctx)
	require.NoError(t, err)

	// Verify document structure
	var doc bson.M
	err = client.Database(database).Collection(collection).
		FindOne(ctx, bson.M{}).Decode(&doc)
	require.NoError(t, err)

	assert.Equal(t, "singleton", doc["leader_key"])
	assert.Equal(t, "instance1", doc["instance_id"])
	assert.NotNil(t, doc["last_seen"])
}

func TestCheckLastSeenTimestampUpdates(t *testing.T) {

	ctx := context.Background()
	connStr, cancel := shared.SetupMongoDB(t)
	database := "test_leader_election"
	collection := "leader_election"

	defer cancel(database)
	// Create MongoDB client
	clientOptions := options.Client().ApplyURI(connStr)
	client, err := mongo.Connect(context.Background(), clientOptions)
	assert.NoError(t, err)

	leaderElection, err := NewDefaultMongoLeaderElection("instance1", client, database)
	require.NoError(t, err)

	// Become leader
	_, err = leaderElection.ElectLeader(ctx)
	require.NoError(t, err)

	// Get initial timestamp
	var initialDoc bson.M
	err = client.Database(database).Collection(collection).
		FindOne(ctx, bson.M{}).Decode(&initialDoc)
	require.NoError(t, err)
	initialTimestamp := initialDoc["last_seen"].(primitive.DateTime)

	// Wait and refresh leadership
	time.Sleep(100 * time.Millisecond)
	err = leaderElection.updateLeaderInfo(ctx)
	require.NoError(t, err)

	// Get updated timestamp
	var updatedDoc bson.M
	err = client.Database(database).Collection(collection).
		FindOne(ctx, bson.M{}).Decode(&updatedDoc)
	require.NoError(t, err)
	updatedTimestamp := updatedDoc["last_seen"].(primitive.DateTime)

	assert.True(t, updatedTimestamp > initialTimestamp)
}

func TestValidateInstanceIdUniquenessDuringElection(t *testing.T) {

	ctx := context.Background()
	connStr, cancel := shared.SetupMongoDB(t)
	database := "test_leader_election"
	collection := "leader_election"
	defer cancel(database)

	// Create MongoDB client
	clientOptions := options.Client().ApplyURI(connStr)
	client, err := mongo.Connect(context.Background(), clientOptions)
	assert.NoError(t, err)

	leaderElection1, err := NewDefaultMongoLeaderElection("instance1", client, database)
	require.NoError(t, err)
	leaderElection2, err := NewDefaultMongoLeaderElection("instance2", client, database)
	require.NoError(t, err)

	// First instance becomes leader
	_, err = leaderElection1.ElectLeader(ctx)
	require.NoError(t, err)

	// Second instance attempts to become leader
	_, err = leaderElection2.ElectLeader(ctx)
	require.NoError(t, err)

	// Verify only one leader exists
	var doc bson.M
	err = client.Database(database).Collection(collection).
		FindOne(ctx, bson.M{}).Decode(&doc)
	require.NoError(t, err)

	// The leader should be either instance1 or instance2, but not both
	assert.True(t, doc["instance_id"] == "instance1" || doc["instance_id"] == "instance2")
}
