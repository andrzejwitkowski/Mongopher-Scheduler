package shared

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/bson"
)

var (
	mongoContainer testcontainers.Container
	mongoOnce      sync.Once
)

func SetupMongoDB(t *testing.T) (string, func(string)) {
	var connStr string
	var err error
	
	mongoOnce.Do(func() {
		ctx := context.Background()
		
		// Start MongoDB container
		req := testcontainers.ContainerRequest{
			Image:        "mongo:latest",
			ExposedPorts: []string{"27017/tcp"},
			WaitingFor:   wait.ForLog("Waiting for connections"),
		}
		
		mongoContainer, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		assert.NoError(t, err)
	})
		
	
	ctx := context.Background()

	// Get connection string
	host, err := mongoContainer.Host(ctx)
	assert.NoError(t, err)
	
	port, err := mongoContainer.MappedPort(ctx, "27017")
	assert.NoError(t, err)
	
	connStr = fmt.Sprintf("mongodb://%s:%s", host, port.Port())
	

	// Cleanup function to remove all collections
	cleanup := func(database_name string) {
		clientOptions := options.Client().ApplyURI(connStr)
		client, err := mongo.Connect(context.Background(), clientOptions)
		assert.NoError(t, err)

		db := client.Database(database_name)
		collections, err := db.ListCollectionNames(context.Background(), bson.M{})
		assert.NoError(t, err)

		for _, coll := range collections {
			err = db.Collection(coll).Drop(context.Background())
			assert.NoError(t, err)
		}
	}

	return connStr, cleanup
}