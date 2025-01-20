# Mongopher-Scheduler

A distributed task scheduler for Go applications with MongoDB and in-memory storage support.

## Features

- Simple and lightweight
- Supports asynchronous task processing
- Built-in retry mechanisms with configurable strategies
- Custom task handlers
- Scheduled tasks with future execution
- Task history tracking
- In-memory and MongoDB storage options
- Concurrent-safe operations
- Distributed leader election with TTL-based failover (in-memory only)

## Leader Election (Low-Level API)

The scheduler includes a distributed leader election mechanism for coordinating multiple instances. **Note:** Currently only available for the in-memory version.

The leader election is used to ensure only one scheduler instance processes tasks at a time in a distributed environment. When multiple scheduler instances are running:

1. The leader instance handles all task processing
2. Non-leader instances remain idle
3. If the leader fails, another instance takes over automatically
4. The leader periodically renews its status to maintain leadership

### Key Features

- **TTL-based leadership** - Leaders must renew their status periodically (default: 30 seconds)
- **Automatic failover** - If a leader fails, another instance will take over
- **Graceful resignation** - Leaders can voluntarily step down
- **Concurrent-safe** - Uses sync.Map for thread-safe operations
- **Mock time provider** - Enables precise testing of time-based operations

### Leader-Aware Scheduler

The LeaderAwareScheduler wraps any TaskScheduler implementation with leader election capabilities. It ensures that only the leader instance processes tasks, while non-leader instances remain idle.

#### Example Usage

```go
// Create leader election instances
leaderElection1 := inmemory.NewLeaderElection("instance-1")
leaderElection2 := inmemory.NewLeaderElection("instance-2")

// Create base schedulers
scheduler1 := inmemory.NewInMemoryTaskScheduler()
scheduler2 := inmemory.NewInMemoryTaskScheduler()

// Wrap with leader awareness
leaderAwareScheduler1 := leader.NewLeaderAwareScheduler(leaderElection1, scheduler1)
leaderAwareScheduler2 := leader.NewLeaderAwareScheduler(leaderElection2, scheduler2)

// Start both schedulers
ctx := context.Background()
if err := leaderAwareScheduler1.StartScheduler(ctx); err != nil {
    log.Fatal(err)
}
if err := leaderAwareScheduler2.StartScheduler(ctx); err != nil {
    log.Fatal(err)
}

// Only the leader will process tasks
_, err := leaderAwareScheduler1.RegisterTask("example-task", nil, nil)
if err != nil {
    log.Fatal(err)
}

// Check leadership status
isLeader, err := leaderElection1.IsLeader(ctx)
if err != nil {
    log.Fatal(err)
}
log.Printf("Instance 1 is leader: %v", isLeader)

// Graceful shutdown
if err := leaderAwareScheduler1.StopScheduler(); err != nil {
    log.Fatal(err)
}
if err := leaderAwareScheduler2.StopScheduler(); err != nil {
    log.Fatal(err)
}
```

#### Key Features

- **Automatic leader election** - Handles leader election and failover automatically
- **Task processing isolation** - Only the leader processes tasks
- **Graceful failover** - Tasks continue processing after leader change
- **Context support** - Proper context propagation for cancellation
- **Concurrent-safe** - Safe for use across multiple goroutines

### Leader Election (Low-Level API)

```go
// Create leader election instance
le := inmemory.NewLeaderElection("instance-1")

// Elect leader
isLeader, err := le.ElectLeader(context.Background())
if err != nil {
    log.Fatal(err)
}

if isLeader {
    log.Println("This instance is now the leader")
    // Start task processing
    scheduler.StartScheduler(ctx)
} else {
    log.Println("Another instance is the leader")
    // Wait for leadership
}

// Check leadership status
isLeader, err = le.IsLeader(context.Background())
if err != nil {
    log.Fatal(err)
}

// Gracefully resign
err = le.Resign(context.Background())
if err != nil {
    log.Fatal(err)
}
```

### Configuration

Leader election can be configured with:

- **TTL Duration** - Time-to-live for leadership (default: 30s)
- **Refresh Interval** - How often leader renews status (default: TTL/2)
- **Instance ID** - Unique identifier for each scheduler instance

## Architecture

The scheduler follows a modular architecture with these core components:

1. **TaskScheduler Interface** - Defines the core scheduling operations
2. **Store Interface** - Abstract storage layer for task persistence
3. **InMemory Implementation** - Non-persistent storage for testing/development
4. **MongoDB Implementation** - Persistent storage for production
5. **Retry Mechanism** - Configurable retry strategies with backoff
6. **Leader Election** - Distributed coordination for multiple instances (in-memory only)

## Usage

### Basic Example

```go
package main

import (
    "context"
    "github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/scheduler/inmemory"
    "github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/store"
    "log"
    "time"
)

func main() {
    // Create new scheduler
    scheduler := inmemory.NewInMemoryTaskScheduler()
    
    // Register task handler
    scheduler.RegisterHandler("example-task", func(task *store.Task[any, int]) error {
        log.Printf("Processing task %d", task.ID)
        return nil
    })
    
    // Start scheduler
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    scheduler.StartScheduler(ctx)
    
    // Schedule new task
    _, err := scheduler.RegisterTask("example-task", nil, nil)
    if err != nil {
        log.Fatal(err)
    }
    
    // Wait for task completion
    done, err := scheduler.WaitForAllTasksToBeDone()
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("All tasks completed: %v", done)
}
```

### Retry Mechanism Example

```go
scheduler.RegisterHandler("retry-task", func(task *store.Task[any, int]) error {
    if task.RetryConfig.Attempts < 3 {
        return fmt.Errorf("simulated failure attempt %d", task.RetryConfig.Attempts + 1)
    }
    return nil
})

// Register task with custom retry config
task := store.Task[any, int]{
    Name: "retry-task",
    RetryConfig: store.RetryConfig{
        MaxRetries: 5,
        StrategyType: store.RetryStrategyBackpressure,
        BaseDelay: 1000, // milliseconds
    },
}
```

### Concurrent Map Usage

The scheduler uses Go's sync.Map for concurrent-safe operations in several areas:

1. **Handler Storage** - Stores registered task handlers
2. **Task Processing** - Tracks in-progress tasks
3. **Status Observers** - Manages task status listeners
4. **Leader Election** - Tracks leader state (in-memory only)

Example sync.Map usage in handler registration:

```go
var handlers sync.Map

func RegisterHandler(name string, handler TaskHandler) {
    handlers.Store(name, handler)
}

func GetHandler(name string) (TaskHandler, bool) {
    if handler, ok := handlers.Load(name); ok {
        return handler.(TaskHandler), true
    }
    return nil, false
}
```

## Configuration

### Retry Strategies

1. **Linear** - Fixed delay between retries
2. **Backpressure** - Exponential backoff with jitter

Configure via RetryConfig:

```go
type RetryConfig struct {
    MaxRetries   int    `bson:"max_retries"`
    StrategyType string `bson:"strategy_type"` // "linear" or "backpressure"
    BaseDelay    int    `bson:"base_delay"`    // in milliseconds
}
```

## Best Practices

1. Use MongoDB storage for production deployments
2. Implement idempotent task handlers
3. Set reasonable retry limits and backoff strategies
4. Monitor task history for error patterns
5. Use context cancellation for graceful shutdowns
6. Implement proper error handling in task handlers
7. Configure appropriate TTL for leader election based on your deployment

## Testing

The scheduler includes comprehensive tests covering:

- Single task execution
- Concurrent task processing
- Retry mechanisms
- Error handling
- Status tracking
- Leader election scenarios:
  - Basic election
  - Failover after TTL expiration
  - Graceful resignation
  - Concurrent elections

Run tests with:

```bash
go test ./...
```

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss proposed changes.