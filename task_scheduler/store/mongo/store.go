package mongo

import (
	"context"
	"fmt"
	"log"
	"mongopher-scheduler/task_scheduler/store"
    "mongopher-scheduler/task_scheduler/shared"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type MongoTask store.Task[bson.M, primitive.ObjectID]

type MongoStore struct {
    collection *mongo.Collection
}

func NewMongoStore(client *mongo.Client, dbName string) *MongoStore {
    return &MongoStore{
        collection: client.Database(dbName).Collection("tasks"),
    }
}

func (ms *MongoStore) InsertTask(ctx context.Context, task MongoTask) (*MongoTask, error) {
    result, err := ms.collection.InsertOne(ctx, task)
    if err != nil {
        return nil, err
    }
    if oid, ok := result.InsertedID.(primitive.ObjectID); ok {
        task.ID = oid
    } else {
        return nil, fmt.Errorf("unexpected ID type: %T", result.InsertedID)
    }
    return &task, nil
}

func (ms *MongoStore) GetTaskByID(ctx context.Context, id primitive.ObjectID) (*MongoTask, error) {
    var task MongoTask
    err := ms.collection.FindOne(ctx, bson.M{"_id": id}).Decode(&task)
    if err != nil {
        if err == mongo.ErrNoDocuments {
            return nil, nil
        }
        return nil, err
    }
    return &task, nil
}

func (ms *MongoStore) DeleteTask(ctx context.Context, id primitive.ObjectID) error {
    _, err := ms.collection.DeleteOne(ctx, bson.M{"_id": id})
    return err
}

func (ms *MongoStore) FindTasksDue(ctx context.Context) ([]MongoTask, error) {
    filter := bson.M{
        "status": bson.M{"$in": []string{
            string(store.StatusNew),
            string(store.StatusRetrying),
        }},
        "$or": []bson.M{
            {"scheduled_at": bson.M{"$exists": false}},
            {"scheduled_at": bson.M{"$lte": time.Now()}},
        },
    }

    cursor, err := ms.collection.Find(ctx, filter)
    if err != nil {
        return nil, err
    }
    defer cursor.Close(ctx)

    var tasks []MongoTask
    if err := cursor.All(ctx, &tasks); err != nil {
        return nil, err
    }
    return tasks, nil
}

func (ms *MongoStore) UpdateTaskStatus(ctx context.Context, id primitive.ObjectID, 
    status store.TaskStatus, errorMsg string) error {
    update := bson.M{
        "$set": bson.M{
            "status": status,
        },
    }
    
    if errorMsg != "" {
        update["$set"].(bson.M)["stack_trace"] = errorMsg
    }
    
    _, err := ms.collection.UpdateByID(ctx, id, update)
    return err
}

func (ts *MongoStore) UpdateTaskState(
	ctx context.Context,
	id primitive.ObjectID,
	status store.TaskStatus,
	errorMsg string,
	retryAttempts int,
	scheduledAt *time.Time) error {

	log.Printf("{GoroutineID: %d} Attempting to update task %s from status to %s", shared.GoroutineID(), id.Hex(), status)

    historyEntry := store.TaskHistory{
        Status:    status,
        Timestamp: time.Now(),
        Error:     errorMsg,
    }

    update := bson.M{
        "$set": bson.M{
            "status":                status,
            "retry_config.attempts": retryAttempts,
        },
        "$push": bson.M{"history": historyEntry},
    }

    if scheduledAt != nil {
        update["$set"].(bson.M)["scheduled_at"] = *scheduledAt
    }

    result, err := ts.collection.UpdateByID(ctx, id, update)
    if err == nil {
        log.Printf("Successfully updated task %s to status %s. Modified count: %d",
            id.Hex(), status, result.ModifiedCount)
        return nil
    }

	return fmt.Errorf("failed to update task state : %v", err)
}

func (ms *MongoStore) UpdateTaskRetry(ctx context.Context, id primitive.ObjectID, 
    attempts int, nextScheduledTime *time.Time) error {
    update := bson.M{
        "$set": bson.M{
            "retry_config.attempts": attempts,
        },
    }
    
    if nextScheduledTime != nil {
        update["$set"].(bson.M)["scheduled_at"] = nextScheduledTime
    }
    
    _, err := ms.collection.UpdateByID(ctx, id, update)
    return err
}

func (ms *MongoStore) AddTaskHistory(ctx context.Context, id primitive.ObjectID, 
    history store.TaskHistory) error {
    update := bson.M{
        "$push": bson.M{
            "history": history,
        },
    }
    
    _, err := ms.collection.UpdateByID(ctx, id, update)
    return err
}

func (ms *MongoStore) GetAllTasks(ctx context.Context) ([]MongoTask, error) {
    cursor := shared.Must(ms.collection.Find(ctx, bson.M{}))
    defer cursor.Close(ctx)

    var tasks []MongoTask
    if err := cursor.All(ctx, &tasks); err != nil {
        return nil, err
    }
    return tasks, nil
}

func (ms *MongoStore) FindTasksInStatus(ctx context.Context, task_status store.TaskStatus) ([]MongoTask, error) {
    filter := bson.M{
        "status": task_status,
    }
    cursor := shared.Must(ms.collection.Find(ctx, filter))
    defer cursor.Close(ctx)

    var tasks []MongoTask
    if err := cursor.All(ctx, &tasks); err != nil {
        return nil, err
    }
    return tasks, nil
}
