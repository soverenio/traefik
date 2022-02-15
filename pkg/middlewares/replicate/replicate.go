package replicate

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/traefik/traefik/v2/pkg/config/runtime"
	"github.com/traefik/traefik/v2/pkg/log"
	"github.com/traefik/traefik/v2/pkg/middlewares"
	"github.com/traefik/traefik/v2/pkg/middlewares/replicate/producer"
	"github.com/traefik/traefik/v2/pkg/middlewares/replicate/utils"
	"github.com/traefik/traefik/v2/pkg/safe"
	"github.com/traefik/traefik/v2/pkg/version"
)

const (
	middlewareType                = "Replicate"
	defaultMaxProcessableBodySize = 1000000
)

var emptyJSONBody = []byte("{}")

// replicate is a middleware used to send copies of requests and responses to an arbitrary service.
type replicate struct {
	sync.RWMutex
	next                   http.Handler
	name                   string
	producer               producer.Producer
	wPool                  *utils.WorkerPool
	maxProcessableBodySize int
	discardedRequests      utils.SyncCounter
	failedRequests         utils.SyncCounter
	successfulRequests     utils.SyncCounter
}

// New creates a new http handler.
func New(ctx context.Context, next http.Handler, config *runtime.MiddlewareInfo, middlewareName string) http.Handler {
	ctx = middlewares.GetLoggerCtx(ctx, middlewareName, middlewareType)
	logger := log.FromContext(ctx)

	logger.Infof("creating middleware %v", middlewareName)
	maxProcessableBodySize := config.Replicate.MaxProcessableBodySize
	if maxProcessableBodySize <= 0 {
		logger.Warnf("maxProcessableBodySize from config equals zero or less, use default value: %d", defaultMaxProcessableBodySize)
		maxProcessableBodySize = defaultMaxProcessableBodySize
	}

	replicate := &replicate{
		next:                   next,
		name:                   middlewareName,
		producer:               nil,
		wPool:                  utils.NewLimitPool(ctx, config.Replicate.WorkerPoolSize),
		maxProcessableBodySize: maxProcessableBodySize,
		discardedRequests:      utils.NewSyncCounter(),
		failedRequests:         utils.NewSyncCounter(),
		successfulRequests:     utils.NewSyncCounter(),
	}
	replicate.wPool.Start()
	logger.Info("worker pool started")

	go replicate.connectProducer(ctx, config, middlewareName, next)
	return replicate
}

func (r *replicate) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	ctx := middlewares.GetLoggerCtx(context.Background(), r.name, middlewareType)
	logger := log.FromContext(ctx)

	eventRequest := producer.Payload{
		Body:    emptyJSONBody,
		Headers: req.Header.Clone(),
	}

	switch {
	case !isSupportedRequestFormat(req.Header.Get("Content-Type")):
		logger.Info("unsupported request Content-Type, setting Event.Request to '{}'")
	case req.ContentLength > int64(r.maxProcessableBodySize):
		logger.Infof("ignoring requests with too long body: body length is %d", req.ContentLength)
		r.discardedRequests.Inc()
		r.next.ServeHTTP(rw, req)
		return
	default:
		body := req.Body
		defer body.Close()

		requestBody, err := ioutil.ReadAll(body)
		if err != nil {
			msg := fmt.Sprintf("error reading request body: %e", err)
			logger.Error(msg)
			r.failedRequests.Inc()
			http.Error(rw, msg, http.StatusInternalServerError)
			return
		}
		req.Body = ioutil.NopCloser(bytes.NewReader(requestBody))

		eventRequest.Body = requestBody
	}

	method := req.Method
	URL := req.URL.String()
	host := req.Host

	remoteAddr, _, err := net.SplitHostPort(req.RemoteAddr)
	if err != nil {
		logger.Warnf("invalid remote address: %s: %v", req.RemoteAddr, err)
		remoteAddr = req.RemoteAddr
	}

	recorder := newResponseRecorder(rw)
	r.next.ServeHTTP(recorder, req)
	responseBody := recorder.GetBody().Bytes()
	responseHeaders := recorder.Header().Clone()
	responseCode := recorder.GetStatusCode()

	_, err = rw.Write(responseBody)
	if err != nil {
		msg := fmt.Sprintf("failed to write response body: %e", err)
		logger.Errorf(msg)
		r.failedRequests.Inc()
		http.Error(rw, msg, http.StatusInternalServerError)
		return
	}

	eventResponse := producer.Payload{
		Body:    emptyJSONBody,
		Headers: responseHeaders,
	}

	if ct := responseHeaders.Get("Content-Type"); strings.Contains(ct, "application/json") {
		eventResponse.Body = responseBody
	} else {
		logger.Info("unsupported response Content-Type, setting Event.Response to '{}'")
	}

	size := len(eventRequest.Body) + len(eventResponse.Body)
	if size > r.maxProcessableBodySize {
		logger.Infof("ignoring request and response with too long body: total length is %d", size)
		r.discardedRequests.Inc()
		return
	}

	r.wPool.Do(func() {
		r.RWMutex.RLock()
		producerInstance := r.producer
		r.RWMutex.RUnlock()

		if producerInstance == nil {
			logger.Warn("has not yet connected to producer (or failed to connect)")
			r.discardedRequests.Inc()
			return
		}

		ev := producer.Event{
			Method:       method,
			URL:          URL,
			Host:         host,
			Client:       remoteAddr,
			Request:      eventRequest,
			Response:     eventResponse,
			ResponseCode: responseCode,
			Time:         time.Now().UTC(),
		}

		if !isVerifyEvent(ev) {
			r.discardedRequests.Inc()
			return
		}

		r.sendEvent(ctx, r.producer, ev, r.name)
	})
}

func isSupportedRequestFormat(format string) bool {
	supportedRequestFormat := []string{
		"application/json",
		"application/x-www-form-urlencoded",
	}

	format = strings.ToLower(format)
	for _, supportedFormat := range supportedRequestFormat {
		if strings.Contains(format, supportedFormat) {
			return true
		}
	}
	return false
}

// StartHeartbeat start regular message sending heartbeat message to kafka for  health checking.
func (r *replicate) StartHeartbeat(ctx context.Context, producer producer.Producer, name string, duration time.Duration) error {
	ctx = log.With(ctx, log.Str("component", "heartbeat"))
	safe.Go(func() {
		r.sendHeartbeat(ctx, producer, name, duration)
	})
	return nil
}

func (r *replicate) connectProducer(ctx context.Context, config *runtime.MiddlewareInfo, middlewareName string, next http.Handler) {
	logger := log.FromContext(ctx)

	producerInstance, err := producer.NewKafkaPublisher(config.Replicate.Topic, config.Replicate.HeartbeatTopic, config.Replicate.Brokers)
	if err != nil {
		logger.Fatalf("failed to create a producer: %v", err)
		return
	}
	producerInstance.Connect(ctx)

	r.RWMutex.Lock()
	r.producer = producerInstance
	r.RWMutex.Unlock()

	err = r.StartHeartbeat(ctx, r.producer, middlewareName, time.Second*10)
	if err != nil {
		logger.Warnf("failed to start sending heartbeat messages: %v", err)
	}
}

func (r *replicate) sendEvent(ctx context.Context, producer producer.Producer, event producer.Event, name string) {
	logger := log.FromContext(ctx)
	err := r.producer.ProduceEvent(event)
	switch {
	case err != nil:
		logger.Warnf("error sending event message to kafka: %v", err)
		r.failedRequests.Inc()
	default:
		r.successfulRequests.Inc()
	}
}

func (r *replicate) sendHeartbeat(ctx context.Context, p producer.Producer, name string, duration time.Duration) {
	logger := log.FromContext(ctx)
	logger.Info("Initial sending heartbeat messages")

	ticker := time.NewTicker(duration)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("stop sending heartbeat messages")
			return
		case <-ticker.C:
			hostname, err := os.Hostname()
			if err != nil {
				logger.Warnf("failed to resolve hostname: %v", err)
			}
			discarded := r.discardedRequests.Load()
			poolDiscarded := r.wPool.LoadDiscarded()
			heartbeat := producer.Heartbeat{
				Host:         hostname,
				Time:         time.Now().UTC(),
				Discarded:    discarded + poolDiscarded,
				Failed:       r.failedRequests.Load(),
				Successful:   r.successfulRequests.Load(),
				BuildVersion: version.Version,
			}
			err = p.ProduceHeartbeat(heartbeat)
			if err != nil {
				logger.Warnf("failed to send message: %v", err)
				r.wPool.AddDiscarded(poolDiscarded)
				r.discardedRequests.Add(discarded)
				r.failedRequests.Add(heartbeat.Failed)
				r.successfulRequests.Add(heartbeat.Successful)
			}
		}
	}
}

func isVerifyEvent(ev producer.Event) bool {
	return ev.Host != "" && ev.Client != ""
}
