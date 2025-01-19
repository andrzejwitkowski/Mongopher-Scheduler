package inmemory

import (
    "context"
    "sync"
    "testing"
    "time"
    
    "github.com/stretchr/testify/assert"
    "github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/scheduler/inmemory"
    "github.com/andrzejwitkowski/Mongopher-Scheduler/task_scheduler/shared"
)

func setup() {
    inmemory.Reset()
}

func TestLeaderElection_HappyPath(t *testing.T) {
    setup()
    timeProvider := shared.NewMockTimeProvider(time.Now())
    le := inmemory.NewLeaderElectionWithTimeProvider("instance-1", timeProvider)
    
    // Become leader
    isLeader, err := le.ElectLeader(context.Background())
    assert.NoError(t, err)
    assert.True(t, isLeader)
    
    // Verify leadership status
    isLeader, err = le.IsLeader(context.Background())
    assert.NoError(t, err)
    assert.True(t, isLeader)
    
    // Advance time within TTL
    timeProvider.AdvanceBy(15 * time.Second)
    
    // Verify still leader
    isLeader, err = le.IsLeader(context.Background())
    assert.NoError(t, err)
    assert.True(t, isLeader)
    
    // Gracefully resign
    err = le.Resign(context.Background())
    assert.NoError(t, err)
    
    // Verify no longer leader
    isLeader, err = le.IsLeader(context.Background())
    assert.NoError(t, err)
    assert.False(t, isLeader)
}

func TestLeaderElection_Failover(t *testing.T) {
    setup()
    startTime := time.Now()
    timeProvider := shared.NewMockTimeProvider(startTime)
    le1 := inmemory.NewLeaderElectionWithTimeProvider("instance-1", timeProvider)
    le2 := inmemory.NewLeaderElectionWithTimeProvider("instance-2", timeProvider)
    
    // First instance becomes leader
    isLeader, err := le1.ElectLeader(context.Background())
    assert.NoError(t, err)
    assert.True(t, isLeader)
    
    // Simulate leader crash by stopping refresh
    le1.CancelRefresh()
    
    // Advance time past TTL
    timeProvider.AdvanceBy(35 * time.Second)
    
    // Second instance should now become leader
    isLeader, err = le2.ElectLeader(context.Background())
    assert.NoError(t, err)
    assert.True(t, isLeader)
    
    // Verify first instance is no longer leader
    isLeader, err = le1.IsLeader(context.Background())
    assert.NoError(t, err)
    assert.False(t, isLeader)
}

func TestLeaderElection_Resign(t *testing.T) {
    setup()
    timeProvider := shared.NewMockTimeProvider(time.Now())
    le1 := inmemory.NewLeaderElectionWithTimeProvider("instance-1", timeProvider)
    le2 := inmemory.NewLeaderElectionWithTimeProvider("instance-2", timeProvider)

    // First instance becomes leader
    timeProvider.AdvanceBy(5 * time.Second)
    isLeader, err := le1.ElectLeader(context.Background())
    assert.NoError(t, err)
    assert.True(t, isLeader)

    // First instance gracefully resigns
    err = le1.Resign(context.Background())
    assert.NoError(t, err)

    // Second instance should now become leader
    timeProvider.AdvanceBy(5 * time.Second)
    isLeader, err = le2.ElectLeader(context.Background())
    assert.NoError(t, err)
    assert.True(t, isLeader)

    // Verify first instance is no longer leader
    isLeader, err = le1.IsLeader(context.Background())
    assert.NoError(t, err)
    assert.False(t, isLeader)

    // Verify first instance cannot re-elect itself
    isLeader, err = le1.ElectLeader(context.Background())
    assert.NoError(t, err)
    assert.False(t, isLeader)
}

func TestLeaderElection_Concurrent(t *testing.T) {
    setup()
    timeProvider := shared.NewMockTimeProvider(time.Now())
    
    le1 := inmemory.NewLeaderElectionWithTimeProvider("instance-1", timeProvider)
    le2 := inmemory.NewLeaderElectionWithTimeProvider("instance-2", timeProvider)
    
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
