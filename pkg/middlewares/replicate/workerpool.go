package replicate

import (
	"context"
	"sync"

	"github.com/traefik/traefik/v2/pkg/log"
)

const defaultPoolSize = 10000

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

func NewLimitPool(parentCtx context.Context, poolSize int) *WPool {
	ctx, cancel := context.WithCancel(parentCtx)
	if poolSize == 0 {
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

func (p *WPool) Start() {
	for i := 0; i < p.workersCount; i++ {
		p.waitGroup.Add(1)

		worker := newWorker(p.ctx, p.jobs)

		go worker.Run()
	}
}

func (p *WPool) Do(makeFunc func()) {
	if len(p.jobs) < p.workersCount {
		p.jobs <- makeFunc
	}

}
func (w *worker) Run() {
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
func (p *WPool) Stop() {
	p.cancel()
	p.waitGroup.Wait()
}
