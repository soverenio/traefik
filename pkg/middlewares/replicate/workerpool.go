package replicate

import (
	"context"
	"strconv"
	"sync"

	"github.com/traefik/traefik/v2/pkg/log"
)

const defaultPoolSize = 10

// WPool is a pool of workers with limit of workers.
type wPool struct {
	ctx          context.Context
	waitGroup    sync.WaitGroup
	workersCount int
	jobs         chan func()
	cancel       context.CancelFunc
}

func (p *wPool) workerLoop(ctx context.Context) {
	defer p.waitGroup.Done()

	for {
		jobFunc, ok := <-p.jobs
		if !ok { // that means channel is closed, stopping processing messages
			log.FromContext(ctx).Debug("stopping worker from worker pool")
			return
		}
		jobFunc()
	}
}

// NewLimitPool create new Worker Pool.
func newLimitPool(parentCtx context.Context, poolSize int) *wPool {
	ctx, cancel := context.WithCancel(parentCtx)
	if poolSize == 0 {
		logger := log.FromContext(ctx)
		logger.Debugf("poolSize equal zero from config, use default pool size %d", defaultPoolSize)
		poolSize = defaultPoolSize
	}

	return &wPool{
		ctx:          ctx,
		workersCount: poolSize,
		jobs:         make(chan func(), poolSize),
		cancel:       cancel,
	}
}

// Start create workers in worker pool and start working.
func (p *wPool) Start() {
	p.waitGroup.Add(p.workersCount)

	for i := 0; i < p.workersCount; i++ {
		ctx := log.With(p.ctx, log.Str("worker", strconv.Itoa(i)))

		go p.workerLoop(ctx)
	}
}

// Do  worker pool does the job.
func (p *wPool) Do(makeFunc func()) {
	select {
	case p.jobs <- makeFunc:
	default:
		// do nothing in case we're overbooked
	}
}

// Stop workers in pool.
func (p *wPool) Stop() {
	// stop taking new jobs and drop existing buffered jobs
	close(p.jobs)

	// if functions inside wPool honors context cancel - give them a 'hint'
	p.cancel()

	// wait until all currently running jobs are processed
	p.waitGroup.Wait()
}
