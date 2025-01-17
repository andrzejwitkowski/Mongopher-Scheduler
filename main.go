package main

import (
	"context"
	"fmt"
	"log"
	
	"mongopher-scheduler/task_scheduler/scheduler/inmemory"
	"mongopher-scheduler/task_scheduler/store"

	"time"
)

func main() {
	// connectionURI := "mongodb://root:example@localhost:30017"
    
	// clientOptions := options.Client().
	// 	ApplyURI(connectionURI).
	// 	SetAuth(options.Credential{
	// 		Username: "root",
	// 		Password: "example",
	// 		AuthSource: "admin",
	// 	}).
	// 	SetServerSelectionTimeout(10 * time.Second).
	// 	SetConnectTimeout(10 * time.Second).
	// 	SetDirect(true).
	// 	SetWriteConcern(writeconcern.Majority()).
	// 	SetReadPreference(readpref.Primary()).
	// 	SetReadConcern(readconcern.Local())
	
    // ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
    // defer cancel()

    // client, err := mongo.Connect(ctx, clientOptions)
    // if err != nil {
    //     log.Fatal("Failed to connect to MongoDB:", err)
    // }

    // err = client.Ping(ctx, readpref.Primary())
    // if err != nil {
    //     log.Fatal("Failed to ping MongoDB:", err)
    // }

    // log.Println("Successfully connected to MongoDB")

    // defer func() {
    //     if err := client.Disconnect(ctx); err != nil {
    //         log.Fatal("Failed to disconnect:", err)
    //     }
    // }()

	// collection := client.Database("task_scheduler").Collection("tasks")
	// _, err = collection.DeleteMany(context.Background(), bson.M{})
	// if err != nil {
	// 	log.Fatal("Failed to clean tasks collection:", err)
	// }

	// task_scheduler := scheduler_mongo.NewMongoTaskScheduler(client, "task_scheduler")

	// // Register a sample task handler
	// task_scheduler.RegisterHandler("sample_task", func(task *store.Task[bson.M, primitive.ObjectID]) error {
	// 	fmt.Println("Processing task:", task.ID)
	// 	// Simulate work
	// 	time.Sleep(2 * time.Second)
	// 	return nil
	// })

	// // Register recoverable task handler
	// task_scheduler.RegisterHandler("recoverable_task", func(task *store.Task[bson.M, primitive.ObjectID]) error {
	// 	fmt.Printf("{Goroutine: %d} Processing recoverable task %s (attempt %d/%d)\n", 
	// 		shared.GoroutineID(), task.ID.Hex(), task.RetryConfig.Attempts+1, task.RetryConfig.MaxRetries)
	// 	time.Sleep(1 * time.Second)
		
	// 	// Fail first 3 attempts, then succeed
	// 	if task.RetryConfig.Attempts < 3 {
	// 		return fmt.Errorf("simulated failure (attempt %d)", task.RetryConfig.Attempts+1)
	// 	}
	// 	return nil
	// })

	in_memory_task_scheduler := inmemory.NewInMemoryTaskScheduler()

	in_memory_task_scheduler.RegisterHandler("sample_in_memory_task", func(task *store.Task[any, int]) error {
		fmt.Println("Processing in memory task:", task.ID)
		// Simulate work
		time.Sleep(2 * time.Second)
		return nil
	})


	// Start the scheduler
	scheduler_ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// task_scheduler.StartScheduler(scheduler_ctx)
	in_memory_task_scheduler.StartScheduler(scheduler_ctx)


	// Register some sample tasks
	// for i := 0; i < 5; i++ {
	// 	_, err := task_scheduler.RegisterTask("sample_task", scheduler.NewBSONParameter(bson.M{"index": i}), nil)
	// 	if err != nil {
	// 		log.Println("Failed to register task:", err)
	// 	}
	// }

	for i := 0; i < 1; i++ {
		// _, err := task_scheduler.RegisterTask("recoverable_task", scheduler.NewBSONParameter(bson.M{"recoverable_index": i}), nil)
		_, err := in_memory_task_scheduler.RegisterTask("sample_in_memory_task", inmemory.NewAnyStructParameter(i), nil)
		if err != nil {
			log.Println("Failed to register task:", err)
		}
	}

	// Wait for all tasks to be done
	done, err := in_memory_task_scheduler.WaitForAllTasksToBeDone()
	if err != nil {
		log.Fatal("Failed to wait for all tasks to be done:", err)
	}
	if !done {
		log.Fatal("Failed to wait for all tasks to be done")
	}

	// Run for a while to process tasks
	// time.Sleep(30 * time.Second)
}
