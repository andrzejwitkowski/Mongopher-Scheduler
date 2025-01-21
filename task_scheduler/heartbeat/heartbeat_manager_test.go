package heartbeat

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestHeartbeatPeriodicExecution(t *testing.T) {
	hm := NewHeartbeatManager(100*time.Millisecond, 50*time.Millisecond)

	var callCount int
	var mu sync.Mutex

	hm.Register(
		func(ctx context.Context) error {
			mu.Lock()
			callCount++
			mu.Unlock()
			return nil
		},
		nil,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	hm.Start(ctx)
	time.Sleep(350 * time.Millisecond) // Allow for 3-4 heartbeats

	mu.Lock()
	if callCount < 3 {
		t.Errorf("Expected at least 3 heartbeats, got %d", callCount)
	}
	mu.Unlock()
}

func TestHeartbeatFailureHandling(t *testing.T) {
	hm := NewHeartbeatManager(100*time.Millisecond, 50*time.Millisecond)

	failCount := 0
	var mu sync.Mutex

	hm.Register(
		func(ctx context.Context) error {
			return errors.New("simulated failure")
		},
		func(ctx context.Context) {
			mu.Lock()
			failCount++
			mu.Unlock()
		},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	hm.Start(ctx)
	time.Sleep(350 * time.Millisecond)

	mu.Lock()
	if failCount < 3 {
		t.Errorf("Expected at least 3 failure callbacks, got %d", failCount)
	}
	mu.Unlock()
}

func TestHeartbeatStop(t *testing.T) {
	hm := NewHeartbeatManager(100*time.Millisecond, 50*time.Millisecond)

	var callCount int
	var mu sync.Mutex

	hm.Register(
		func(ctx context.Context) error {
			mu.Lock()
			callCount++
			mu.Unlock()
			return nil
		},
		nil,
	)

	ctx := context.Background()
	hm.Start(ctx)

	time.Sleep(150 * time.Millisecond)
	hm.Stop()

	initialCount := callCount
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	if callCount != initialCount {
		t.Error("Heartbeat continued after Stop()")
	}
	mu.Unlock()
}

func TestHeartbeatContextCancellation(t *testing.T) {
	hm := NewHeartbeatManager(100*time.Millisecond, 50*time.Millisecond)

	var callCount int
	var mu sync.Mutex

	hm.Register(
		func(ctx context.Context) error {
			mu.Lock()
			callCount++
			mu.Unlock()
			return nil
		},
		nil,
	)

	ctx, cancel := context.WithCancel(context.Background())
	hm.Start(ctx)

	time.Sleep(150 * time.Millisecond)
	cancel()

	initialCount := callCount
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	if callCount != initialCount {
		t.Error("Heartbeat continued after context cancellation")
	}
	mu.Unlock()
}

func TestHeartbeatConcurrentAccess(t *testing.T) {
	hm := NewHeartbeatManager(100*time.Millisecond, 50*time.Millisecond)

	var wg sync.WaitGroup
	wg.Add(3)

	// Test concurrent registration
	go func() {
		defer wg.Done()
		hm.Register(
			func(ctx context.Context) error { return nil },
			func(ctx context.Context) {},
		)
	}()

	// Test concurrent start
	go func() {
		defer wg.Done()
		hm.Start(context.Background())
	}()

	// Test concurrent stop
	go func() {
		defer wg.Done()
		hm.Stop()
	}()

	wg.Wait()
	// If we get here without panicking, the test passes
}
