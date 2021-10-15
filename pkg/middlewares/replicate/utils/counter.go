package utils

import (
	"sync/atomic"
)

type SyncCounter interface {
	Inc()
	Add(uint64)
	Load() uint64
}
type syncCounter struct {
	count uint64
}

func (c *syncCounter) Load() uint64 {
	for {
		val := atomic.LoadUint64(&c.count)
		if atomic.CompareAndSwapUint64(&c.count, val, 0) {
			return val
		}
	}
}

func (c *syncCounter) Inc() {
	atomic.AddUint64(&c.count, 1)
}

func (c *syncCounter) Add(delta uint64) {
	atomic.AddUint64(&c.count, delta)
}

func NewSyncCounter() SyncCounter {
	return &syncCounter{}
}
