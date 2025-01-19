package shared

import (
    "sync"
    "time"
)

type TimeProvider interface {
    Now() time.Time
    AdvanceBy(duration time.Duration)
    NowIs(t time.Time)
}

type realTimeProvider struct{}

func (r *realTimeProvider) Now() time.Time {
    return time.Now()
}

func (r *realTimeProvider) AdvanceBy(duration time.Duration) {
    // No-op in real implementation
}

func (r *realTimeProvider) NowIs(t time.Time) {
    // No-op in real implementation
}

type mockTimeProvider struct {
    mu    sync.Mutex
    now   time.Time
}

func NewMockTimeProvider(initial time.Time) *mockTimeProvider {
    return &mockTimeProvider{
        now: initial,
    }
}

func (m *mockTimeProvider) Now() time.Time {
    m.mu.Lock()
    defer m.mu.Unlock()
    return m.now
}

func (m *mockTimeProvider) AdvanceBy(duration time.Duration) {
    m.mu.Lock()
    defer m.mu.Unlock()
    m.now = m.now.Add(duration)
}

func (m *mockTimeProvider) NowIs(t time.Time) {
    m.mu.Lock()
    defer m.mu.Unlock()
    m.now = t
}

var (
    defaultTimeProvider TimeProvider = &realTimeProvider{}
    currentTimeProvider TimeProvider = defaultTimeProvider
    timeProviderMu      sync.Mutex
)

func Now() time.Time {
    timeProviderMu.Lock()
    defer timeProviderMu.Unlock()
    return currentTimeProvider.Now()
}

func SetTimeProvider(provider TimeProvider) {
    timeProviderMu.Lock()
    defer timeProviderMu.Unlock()
    currentTimeProvider = provider
}

func ResetTimeProvider() {
    timeProviderMu.Lock()
    defer timeProviderMu.Unlock()
    currentTimeProvider = defaultTimeProvider
}

func DefaultTimeProvider() TimeProvider {
    timeProviderMu.Lock()
    defer timeProviderMu.Unlock()
    return currentTimeProvider
}