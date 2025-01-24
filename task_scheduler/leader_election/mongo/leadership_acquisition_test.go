package leader_election

import (
	"context"
	"testing"
	"time"
	"sync"

	"github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/shared"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDocumentVersionConflicts(t *testing.T) {
	connStr, cleanup := shared.SetupMongoDB(t)
	defer cleanup("testdb")

	client := setupClient(t, connStr)
	le1, le2 := createElectionPair(t, client)

	// Simulate concurrent document updates
	var wg sync.WaitGroup
	wg.Add(2)
	
	var successCount int
	var errCount int
	
	go func() {
		defer wg.Done()
		ok, err := le1.ElectLeader(context.Background())
		if ok && err == nil {
			successCount++
		} else if !ok {
			errCount++
		}
	}()

	go func() {
		defer wg.Done()
		ok, err := le2.ElectLeader(context.Background())
		if ok && err == nil {
			successCount++
		} else if !ok {
			errCount++
		}
	}()

	wg.Wait()
	
	// Verify exactly one leader was elected
	assert.Equal(t, 1, successCount, "Exactly one instance should succeed")
	assert.Equal(t, 1, errCount, "One instance should get duplicate key error")
}

func TestTTLExpiration(t *testing.T) {
	connStr, cleanup := shared.SetupMongoDB(t)
	defer cleanup("testdb")

	client := setupClient(t, connStr)
	le, _ := NewMongoLeaderElection("ttl-test", client, "testdb", 2 * time.Second)

	// Acquire leadership
	ok, err := le.ElectLeader(context.Background())
	require.NoError(t, err)
	require.True(t, ok)

	// Verify leadership document
	coll := client.Database("testdb").Collection("leader_election")
	var doc bson.M
	err = coll.FindOne(context.Background(), bson.M{"leader_key": "singleton"}).Decode(&doc)
	require.NoError(t, err)
	assert.NotNil(t, doc["last_seen"], "Document should have last_seen field")

	// Wait for TTL expiration (default is 30s)
	time.Sleep(5 * time.Second)
	
	// Verify leadership expired
	isLeader, err := le.IsLeader(context.Background())
	assert.NoError(t, err)
	assert.False(t, isLeader, "Leadership should expire after TTL")
}

func TestDocumentStructureValidation(t *testing.T) {
	connStr, cleanup := shared.SetupMongoDB(t)
	defer cleanup("testdb")

	client := setupClient(t, connStr)
	
	// Test various invalid configurations
	tests := []struct {
		name       string
		instanceID string
		client     *mongo.Client
		database   string
	}{
		{
			name:       "empty instance ID",
			instanceID: "",
			client:     client,
			database:   "testdb",
		},
		{
			name:       "nil client",
			instanceID: "test-instance",
			client:     nil,
			database:   "testdb",
		},
		{
			name:       "empty database",
			instanceID: "test-instance",
			client:     client,
			database:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewDefaultMongoLeaderElection(tt.instanceID, tt.client, tt.database)
			assert.Error(t, err, "Should reject invalid configuration: %s", tt.name)

			// Verify collection is empty
			coll := client.Database("testdb").Collection("leader_election")
			count, err := coll.CountDocuments(context.Background(), bson.M{})
			require.NoError(t, err)
			assert.Equal(t, int64(0), count, "No documents should be created with invalid configuration: %s", tt.name)
		})
	}
}

func setupClient(t *testing.T, connStr string) *mongo.Client {
	clientOptions := options.Client().ApplyURI(connStr)
	client, err := mongo.Connect(context.Background(), clientOptions)
	require.NoError(t, err)
	return client
}

func createElectionPair(t *testing.T, client *mongo.Client) (*MongoLeaderElection, *MongoLeaderElection) {
	le1, err := NewDefaultMongoLeaderElection("instance-1", client, "testdb")
	require.NoError(t, err)
	
	le2, err := NewDefaultMongoLeaderElection("instance-2", client, "testdb")
	require.NoError(t, err)
	
	return le1, le2
}