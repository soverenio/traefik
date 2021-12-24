package utils

import (
	"context"
	"strconv"
	"sync"

	"github.com/traefik/traefik/v2/pkg/log"
)

// DefaultPoolSize is a default pool size.
const DefaultPoolSize = 10

// WorkerPool is a pool of workers with limit of workers.
type WorkerPool struct {
	ctx             context.Context
	workerWaitGroup sync.WaitGroup
	workerCount     int
	jobs            *syncWriteQueue
	cancel          context.CancelFunc
}

func (p *WorkerPool) workerLoop(ctx context.Context) {
	defer p.workerWaitGroup.Done()
	queue := p.jobs.ReadQueue()
	logger := log.FromContext(ctx)
	logger.Info("worker started")

	if queue == nil { // that means channel was closed, stopping processing messages
		logger.Info("stopping worker")
		return
	}

	for {
		jobFunc, ok := <-queue
		if !ok { // that means channel is closed, stopping processing messages
			logger.Info("stopping worker")
			return
		}
		jobFunc()
	}
}

// NewLimitPool create new Worker Pool.
func NewLimitPool(parentCtx context.Context, poolSize int) *WorkerPool {
	ctx, cancel := context.WithCancel(parentCtx)
	ctx = log.With(ctx, log.Str("component", "workerpool"))
	logger := log.FromContext(ctx)

	if poolSize == 0 {
		logger.Warnf("poolSize equal zero from config, use default pool size %d", DefaultPoolSize)
		poolSize = DefaultPoolSize
	}

	return &WorkerPool{
		ctx:         ctx,
		workerCount: poolSize,
		jobs:        newSyncWriteQueue(poolSize),
		cancel:      cancel,
	}
}

// Start create workers in worker pool and start working.
func (p *WorkerPool) Start() {
	p.workerWaitGroup.Add(p.workerCount)

	for i := 0; i < p.workerCount; i++ {
		ctx := log.With(p.ctx, log.Str("worker", strconv.Itoa(i)))
		go p.workerLoop(ctx)
	}
}

// Do appends job to execution queue.
func (p *WorkerPool) Do(makeFunc func()) {
	p.jobs.Enqueue(makeFunc)
}

// LoadDiscarded returns count of discarded jobs.
func (p *WorkerPool) LoadDiscarded() uint64 {
	return p.jobs.discarded.Load()
}

// AddDiscarded adds delta to discarded jobs counter.
func (p *WorkerPool) AddDiscarded(delta uint64) {
	p.jobs.discarded.Add(delta)
}

// Stop workers in pool.
func (p *WorkerPool) Stop() {
	// stop taking new jobs and drop existing buffered jobs
	p.jobs.Close()

	// if functions inside WorkerPool honors context cancel - give them a 'hint'
	p.cancel()

	// wait until all currently running jobs are processed
	p.workerWaitGroup.Wait()
}
