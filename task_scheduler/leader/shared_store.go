package leader

import (
	"sync"
	"time"
)

var (
	leaderStore     *sync.Map
	leaderStoreOnce sync.Once
	storeMutex      sync.Mutex
)

type leaderInfo struct {
	instanceID string
	lastSeen   time.Time
}

func GetLeaderStore() *sync.Map {
	leaderStoreOnce.Do(func() {
		leaderStore = &sync.Map{}
	})
	return leaderStore
}

func WithStoreLock(fn func()) {
	storeMutex.Lock()
	defer storeMutex.Unlock()
	fn()
}

func Reset() {
	leaderStoreOnce = sync.Once{}
	leaderStore = nil
}
