package utils

import (
	"sync"
)

// syncWriteQueue is a safe multi-writer, multi-reader and separate closer queue
// it sacrifices speed (not much) for safe stops:
// we're using sync.RWMutex to separate queue pushes (== atomic.AddInt,
// it won't throttle much) from close(queue).
type syncWriteQueue struct {
	writeMutex sync.RWMutex
	queue      chan func()
}

// Enqueue appends element to queue (in case it isn't closed)
// it won't enqueue in case:
// 1. queue is overbooked
// 2. we've already closed queue
func (s *syncWriteQueue) Enqueue(job func()) {
	s.writeMutex.RLock()
	defer s.writeMutex.RUnlock()

	if s.queue == nil {
		return
	}

	select {
	case s.queue <- job:
	default:
		// do nothing
	}
}

func (s *syncWriteQueue) Close() {
	s.writeMutex.Lock()
	defer s.writeMutex.Unlock()

	close(s.queue)
	s.queue = nil
}

func (s *syncWriteQueue) ReadQueue() <-chan func() {
	return s.queue
}

func newSyncWriteQueue(len int) *syncWriteQueue {
	if len < 1 {
		panic("illegal value")
	}
	return &syncWriteQueue{
		queue: make(chan func(), len),
	}
}
