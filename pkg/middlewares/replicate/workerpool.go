package replicate

import (
	"context"
	"sync"

	"github.com/traefik/traefik/v2/pkg/log"
)

const defaultPoolSize = 10

// WPool is  pool of workers with limit of workers.
type WPool struct {
	ctx          context.Context
	waitGroup    sync.WaitGroup
	workersCount int
	jobs         chan func()
	cancel       context.CancelFunc
}

type worker struct {
	ctx  context.Context
	jobs chan func()
}

// NewLimitPool create new Worker Pool.
func NewLimitPool(parentCtx context.Context, poolSize int) *WPool {
	ctx, cancel := context.WithCancel(parentCtx)
	if poolSize == 0 {
		logger := log.FromContext(ctx)
		logger.Debugf("poolSize equal zero from config, use default pool size %d", defaultPoolSize)
		poolSize = defaultPoolSize
	}

	return &WPool{
		ctx:          ctx,
		workersCount: poolSize,
		jobs:         make(chan func(), poolSize),
		cancel:       cancel,
	}
}

func newWorker(ctx context.Context, jobs chan func()) *worker {
	return &worker{
		ctx:  ctx,
		jobs: jobs,
	}
}

// Start create workers in worker pool and start working.
func (p *WPool) Start() {
	for i := 0; i < p.workersCount; i++ {
		p.waitGroup.Add(1)
		worker := newWorker(p.ctx, p.jobs)

		go worker.run()
	}
}

// Do  worker pool does the job.
func (p *WPool) Do(makeFunc func()) {
	if len(p.jobs) < p.workersCount {
		p.jobs <- makeFunc
	}
}
func (w *worker) run() {
	for {
		select {
		case jobFunc, ok := <-w.jobs:
			if !ok {
				continue
			}
			jobFunc()

		case <-w.ctx.Done():
			logger := log.FromContext(w.ctx)
			logger.Debug("Stopping worker from worker pool")
			return
		}
	}
}

// Stop workers in pool.
func (p *WPool) Stop() {
	p.cancel()
	p.waitGroup.Wait()
}
