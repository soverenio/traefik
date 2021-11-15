package main

import (
	"context"
	"net/http"

	"github.com/traefik/traefik/v2/pkg/log"

	"github.com/traefik/traefik/v2/pkg/metrics"

	"github.com/traefik/traefik/v2/pkg/safe"
)

type MetricServer struct {
	routinesPool *safe.Pool
	srv          *http.Server
}

func NewMetricServer(cfg *StaticConfiguration,
	routinesPool *safe.Pool,
) *MetricServer {
	var srv *http.Server
	if cfg.Metrics != nil && cfg.Metrics.Prometheus != nil {
		metricsHandler := metrics.PrometheusHandler()
		if ep, ok := cfg.EntryPoints[cfg.Metrics.Prometheus.EntryPoint]; ok {
			mux := http.NewServeMux()
			mux.Handle("/metrics", metricsHandler)
			srv = &http.Server{
				Handler: mux,
				Addr:    ep.Address,
			}
		}

	}
	return &MetricServer{routinesPool: routinesPool, srv: srv}
}

func (s *MetricServer) Start() {
	s.routinesPool.GoCtx(s.listen)
}

func (s *MetricServer) listen(ctx context.Context) {
	go func() {
		if s.srv == nil {
			return
		}
		err := s.srv.ListenAndServe()
		if err != nil {
			log.WithoutContext().Error(err)
		}
	}()
	for {
		select {
		case <-ctx.Done():
			if s.srv != nil {
				err := s.srv.Shutdown(context.Background())
				if err != nil {
					log.WithoutContext().Error(err)
				}
			}
			return
		}
	}
}
