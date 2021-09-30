package replicate

import (
	"bytes"
	"context"
	"errors"
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
)

const (
	middlewareType                = "Replicate"
	emptyJSONBody                 = "{}"
	defaultMaxProcessableBodySize = 1000000
)

// replicate is a middleware used to send copies of requests and responses to an arbitrary service.
type replicate struct {
	sync.RWMutex
	next                   http.Handler
	name                   string
	producer               producer.Producer
	wPool                  *utils.WorkerPool
	maxProcessableBodySize int
}

// New creates a new http handler.
func New(ctx context.Context, next http.Handler, config *runtime.MiddlewareInfo, middlewareName string) http.Handler {
	ctx = middlewares.GetLoggerCtx(ctx, middlewareName, middlewareType)
	logger := log.FromContext(ctx)

	logger.Debugf("Creating middleware %v", middlewareName)
	maxProcessableBodySize := config.Replicate.MaxProcessableBodySize
	if maxProcessableBodySize <= 0 {
		logger.Debugf("maxProcessableBodySize from config equals zero or less, use default value: %d", defaultMaxProcessableBodySize)
		maxProcessableBodySize = defaultMaxProcessableBodySize
	}

	replicate := &replicate{
		next:                   next,
		name:                   middlewareName,
		producer:               nil,
		wPool:                  utils.NewLimitPool(ctx, config.Replicate.WorkerPoolSize),
		maxProcessableBodySize: maxProcessableBodySize,
	}
	replicate.wPool.Start()
	logger.Debug("worker pool started")

	go replicate.connectProducer(ctx, config, middlewareName, next)
	return replicate
}

func (r *replicate) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	ctx := middlewares.GetLoggerCtx(context.Background(), r.name, middlewareType)
	logger := log.FromContext(ctx)

	eventRequest := producer.Payload{
		Body:    emptyJSONBody,
		Headers: map[string][]string{},
	}

	switch {
	case !strings.Contains(req.Header.Get("Content-Type"), "application/json"):
		logger.Debug("ignoring requests with header 'Content-Type' not 'application/json', setting Event.Request to '{}'")
	case req.ContentLength > int64(r.maxProcessableBodySize):
		logger.Debugf("ignoring requests with too long body: body length is %d", req.ContentLength)
		r.next.ServeHTTP(rw, req)
		return
	default:
		body := req.Body
		defer body.Close()

		requestBody, err := ioutil.ReadAll(body)
		if err != nil {
			msg := fmt.Sprintf("error reading request body: %e", err)
			logger.Debug(msg)
			http.Error(rw, msg, http.StatusInternalServerError)
			return
		}
		req.Body = ioutil.NopCloser(bytes.NewReader(requestBody))

		eventRequest.Body = string(requestBody)
		eventRequest.Headers = req.Header
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
	responseHeaders := recorder.Header()

	_, err = rw.Write(responseBody)
	if err != nil {
		msg := fmt.Sprintf("failed to write response body: %e", err)
		logger.Debugf(msg)
		http.Error(rw, msg, http.StatusInternalServerError)
		return
	}

	var eventResponse producer.Payload
	if ct := responseHeaders.Get("Content-Type"); strings.Contains(ct, "application/json") {
		eventResponse = producer.Payload{
			Body:    string(responseBody),
			Headers: responseHeaders,
		}
	} else {
		logger.Debug("ignoring responses with header 'Content-Type' not 'application/json', setting Event.Response to '{}'")
		eventResponse = producer.Payload{
			Body:    emptyJSONBody,
			Headers: map[string][]string{},
		}
	}

	size := len(eventRequest.Body) + len(eventResponse.Body)
	if size > r.maxProcessableBodySize {
		logger.Debugf("ignoring request and response with too long body: total length is %d", size)
		return
	}

	r.wPool.Do(func() {
		r.RWMutex.RLock()
		producerInstance := r.producer
		r.RWMutex.RUnlock()

		if producerInstance == nil {
			logger.Warn("has not yet connected to producer (or failed to connect)")
			return
		}

		ev := producer.Event{
			Method:   method,
			URL:      URL,
			Host:     host,
			Client:   remoteAddr,
			Request:  eventRequest,
			Response: eventResponse,
			Time:     time.Now().UTC(),
		}

		if !isVerifyEvent(ev) {
			return
		}

		sendEvent(ctx, r.producer, ev, r.name)
	})
}

// StartAlive start regular message sending alive message to kafka for  health checking.
func StartAlive(ctx context.Context, producer producer.Producer, name string, topic string, duration time.Duration) error {
	ctx = log.With(ctx, log.Str("component", "alive"))
	if topic == "" {
		return errors.New("topic is required")
	}
	safe.Go(func() {
		sendAlive(ctx, producer, name, topic, duration)
	})
	return nil
}

func (r *replicate) connectProducer(ctx context.Context, config *runtime.MiddlewareInfo, middlewareName string, next http.Handler) {
	logger := log.FromContext(ctx)

	producerInstance, err := producer.NewKafkaPublisher(config.Replicate.Topic, config.Replicate.Brokers)
	if err != nil {
		logger.Fatalf("failed to create a producer: %v", err)
		return
	}
	producerInstance.Connect(ctx)

	r.RWMutex.Lock()
	r.producer = producerInstance
	r.RWMutex.Unlock()

	err = StartAlive(ctx, r.producer, middlewareName, config.Replicate.AliveTopic, time.Second*10)
	if err != nil {
		logger.Warnf("failed to start sending alive messages: %v", err)
	}
}

func sendEvent(ctx context.Context, producer producer.Producer, event producer.Event, name string) {
	logger := log.FromContext(ctx)
	err := producer.Produce(event)
	if err != nil {
		logger.Warnf("error sending event message to kafka: %v", err)
	}
}

func sendAlive(ctx context.Context, p producer.Producer, name string, topic string, duration time.Duration) {
	logger := log.FromContext(ctx)
	logger.Debug("Initial sending alive messages")

	ticker := time.NewTicker(duration)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Debug("Stopping sending alive messages")
			return
		case <-ticker.C:
			hostname, err := os.Hostname()
			if err != nil {
				logger.Warnf("failed to resolve hostname: %v", err)
			}
			err = p.ProduceTo(producer.Event{
				Host: hostname,
				Time: time.Now().UTC(),
			}, topic)
			if err != nil {
				logger.Warnf("failed to send message: %v", err)
			}
		}
	}
}

func isVerifyEvent(ev producer.Event) bool {
	return ev.Host != "" && ev.Client != ""
}
