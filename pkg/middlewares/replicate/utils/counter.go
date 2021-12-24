package utils

import (
	"sync/atomic"
)

// SyncCounter safely counts events in parallel threads.
type SyncCounter interface {
	Inc()
	Add(uint64)
	Load() uint64
}

type syncCounter struct {
	count uint64
}

// Load returns current value and reset it.
func (c *syncCounter) Load() uint64 {
	for {
		val := atomic.LoadUint64(&c.count)
		if atomic.CompareAndSwapUint64(&c.count, val, 0) {
			return val
		}
	}
}

// Inc increments counter.
func (c *syncCounter) Inc() {
	atomic.AddUint64(&c.count, 1)
}

// Add adds delta to counter.
func (c *syncCounter) Add(delta uint64) {
	atomic.AddUint64(&c.count, delta)
}

// NewSyncCounter create new SyncCounter.
func NewSyncCounter() SyncCounter {
	return &syncCounter{}
}
