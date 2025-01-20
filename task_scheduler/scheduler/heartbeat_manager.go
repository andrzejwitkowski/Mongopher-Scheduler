package scheduler

import (
	"context"
	"sync"
	"time"
)

type HeartbeatManager interface {
	Start(ctx context.Context)
	Stop()
	Register(checkFunc func(ctx context.Context) error, onFailFunc func(ctx context.Context))
}

type heartbeatManager struct {
	interval   time.Duration
	timeout    time.Duration
	checkFunc  func(ctx context.Context) error
	onFailFunc func(ctx context.Context)
	cancelFunc context.CancelFunc
	mu         sync.Mutex
}

func NewHeartbeatManager(interval, timeout time.Duration) HeartbeatManager {
	return &heartbeatManager{
		interval: interval,
		timeout:  timeout,
	}
}

func (hm *heartbeatManager) Start(ctx context.Context) {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	if hm.cancelFunc != nil {
		return // Already running
	}

	ctx, cancel := context.WithCancel(ctx)
	hm.cancelFunc = cancel

	go hm.run(ctx)
}

func (hm *heartbeatManager) Stop() {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	if hm.cancelFunc != nil {
		hm.cancelFunc()
		hm.cancelFunc = nil
	}
}

func (hm *heartbeatManager) Register(checkFunc func(ctx context.Context) error, onFailFunc func(ctx context.Context)) {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	hm.checkFunc = checkFunc
	hm.onFailFunc = onFailFunc
}

func (hm *heartbeatManager) run(ctx context.Context) {
	ticker := time.NewTicker(hm.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			hm.mu.Lock()
			checkFunc := hm.checkFunc
			onFailFunc := hm.onFailFunc
			hm.mu.Unlock()

			if checkFunc != nil {
				ctx, cancel := context.WithTimeout(ctx, hm.timeout)
				if err := checkFunc(ctx); err != nil {
					if onFailFunc != nil {
						onFailFunc(ctx)
					}
				}
				cancel()
			}
		case <-ctx.Done():
			return
		}
	}
}
