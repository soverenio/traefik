package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"time"

	"github.com/traefik/traefik/v2/pkg/log"
	"github.com/traefik/traefik/v2/pkg/safe"
)

func (s *Daemon) Start(ctx context.Context) {
	go func() {
		<-ctx.Done()
		logger := log.FromContext(ctx)
		logger.Info("I have to go...")
		logger.Info("Stopping server gracefully")
		s.Stop()
	}()

	s.watcher.Start()
}

// Wait blocks until the server shutdown.
func (s *Daemon) Wait() {
	<-s.stopChan
}

func (s *Daemon) Stop() {
	defer log.WithoutContext().Info("Daemon stopped")

	s.stopChan <- true
}

func (s *Daemon) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	go func(ctx context.Context) {
		<-ctx.Done()
		if errors.Is(ctx.Err(), context.Canceled) {
			return
		} else if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			panic("Timeout while stopping traefik, killing instance ✝")
		}
	}(ctx)

	s.routinesPool.Stop()

	signal.Stop(s.signals)
	close(s.signals)

	close(s.stopChan)

	cancel()
}

// Daemon is the configuration injector daemon.
type Daemon struct {
	watcher *ConfigurationWatcher

	signals  chan os.Signal
	stopChan chan bool

	routinesPool *safe.Pool
}
