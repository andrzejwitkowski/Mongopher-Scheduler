package main

import (
	"context"
	"fmt"
	"log"
	"mongopher-scheduler/task_scheduler"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

func main() {
	connectionURI := "mongodb://root:example@localhost:30017"
    
	clientOptions := options.Client().
		ApplyURI(connectionURI).
		SetAuth(options.Credential{
			Username: "root",
			Password: "example",
			AuthSource: "admin",
		}).
		SetServerSelectionTimeout(10 * time.Second).
		SetConnectTimeout(10 * time.Second).
		SetDirect(true).
		SetWriteConcern(writeconcern.Majority()).
		SetReadPreference(readpref.Primary()).
		SetReadConcern(readconcern.Local())
	
    ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
    defer cancel()

    client, err := mongo.Connect(ctx, clientOptions)
    if err != nil {
        log.Fatal("Failed to connect to MongoDB:", err)
    }

    err = client.Ping(ctx, readpref.Primary())
    if err != nil {
        log.Fatal("Failed to ping MongoDB:", err)
    }

    log.Println("Successfully connected to MongoDB")

    defer func() {
        if err := client.Disconnect(ctx); err != nil {
            log.Fatal("Failed to disconnect:", err)
        }
    }()

	// Clean tasks collection
	collection := client.Database("task_scheduler").Collection("tasks")
	_, err = collection.DeleteMany(context.Background(), bson.M{})
	if err != nil {
		log.Fatal("Failed to clean tasks collection:", err)
	}

	// Create task scheduler instance
	scheduler, err := task_scheduler.NewTaskScheduler(client, "task_scheduler")
	if err != nil {
		log.Fatal(err)
	}

	// Register a sample task handler
	scheduler.RegisterHandler("sample_task", func(task *task_scheduler.Task) error {
		fmt.Println("Processing task:", task.ID)
		// Simulate work
		time.Sleep(2 * time.Second)
		// Simulate random success/failure
		if time.Now().Unix()%2 == 0 {
			return fmt.Errorf("simulated error")
		}
		return nil
	})

	// Register recoverable task handler
	scheduler.RegisterHandler("recoverable_task", func(task *task_scheduler.Task) error {
		fmt.Printf("Processing recoverable task %s (attempt %d/%d)\n", 
			task.ID.Hex(), task.RetryConfig.Attempts+1, task.RetryConfig.MaxRetries)
		time.Sleep(1 * time.Second)
		
		// Fail first 3 attempts, then succeed
		if task.RetryConfig.Attempts < 3 {
			return fmt.Errorf("simulated failure (attempt %d)", task.RetryConfig.Attempts+1)
		}
		return nil
	})

	// Start the scheduler
	scheduler_ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scheduler.StartScheduler(scheduler_ctx)

	// Register some sample tasks
	for i := 0; i < 5; i++ {
		_, err := scheduler.RegisterTask("sample_task", bson.M{"index": i}, nil)
		if err != nil {
			log.Println("Failed to register task:", err)
		}
	}

	for i := 0; i < 5; i++ {
		_, err := scheduler.RegisterTask("recoverable_task", bson.M{"recoverable_index": i}, nil)
		if err != nil {
			log.Println("Failed to register task:", err)
		}
	}

	// Run for a while to process tasks
	time.Sleep(30 * time.Second)
}
