package replicate

import (
	"bytes"
	"context"
	"errors"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/traefik/traefik/v2/pkg/config/runtime"
	"github.com/traefik/traefik/v2/pkg/log"
	"github.com/traefik/traefik/v2/pkg/middlewares"
	"github.com/traefik/traefik/v2/pkg/safe"
)

const (
	typeName = "Replicate"
)

// replicate is a middleware used to send copies of requests and responses to an arbitrary service
type replicate struct {
	sync.RWMutex
	next     http.Handler
	name     string
	producer Producer
	config   *runtime.MiddlewareInfo
	wPool    *WPool
}

// New creates a new http handler.
func New(ctx context.Context, config *runtime.MiddlewareInfo, middlewareName string, next http.Handler) (http.Handler, error) {
	log.FromContext(middlewares.GetLoggerCtx(ctx, middlewareName, typeName)).Debug("Creating middleware")

	replicate := &replicate{
		next:     next,
		name:     middlewareName,
		config:   config,
		producer: nil,
		wPool:    NewLimitPool(ctx, config.Replicate.WorkerPoolSize),
	}
	replicate.wPool.Start()

	go replicate.connectProducer(ctx, config, middlewareName, next)
	return replicate, nil
}

func (r *replicate) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	ctx := context.Background()
	logger := log.FromContext(middlewares.GetLoggerCtx(ctx, r.name, typeName))

	body := req.Body
	defer body.Close()

	method := req.Method
	URL := req.URL.String()
	host := req.Host
	remoteAddr := req.RemoteAddr
	requestHeaders := req.Header

	requestBody, err := ioutil.ReadAll(body)
	if err != nil {
		logger.Debug(err)
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	req.Body = ioutil.NopCloser(bytes.NewReader(requestBody))

	recorder := newResponseRecorder(rw)
	r.next.ServeHTTP(recorder, req)
	responseBody := recorder.GetBody().Bytes()
	responseHeaders := recorder.Header()

	_, err = rw.Write(responseBody)
	if err != nil {
		logger.Debug(err)
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	r.RWMutex.RLock()
	if r.producer == nil {
		logger.Warn("Connect to kafka failed")
		return
	}
	r.RWMutex.RUnlock()
	r.wPool.Do(func() {
		sendEvent(ctx, r.producer, Event{
			Method: method,
			URL:    URL,
			Host:   host,
			Client: remoteAddr,
			Request: Payload{
				Body:    string(requestBody),
				Headers: requestHeaders,
			},
			Response: Payload{
				Body:    string(responseBody),
				Headers: responseHeaders,
			},
			Time: time.Now().UTC(),
		}, r.name)
	})
}

func StartAlive(ctx context.Context, producer Producer, name string, topic string, duration time.Duration) error {
	if topic == "" {
		return errors.New("topic is required")
	}
	safe.Go(func() {
		sendAlive(ctx, producer, name, topic, duration)
	})
	return nil
}

func (r *replicate) connectProducer(ctx context.Context, config *runtime.MiddlewareInfo, middlewareName string, next http.Handler) {

	producer, err := NewKafkaPublisher(config.Replicate.Topic, config.Replicate.Brokers)
	if err != nil {
		log.FromContext(middlewares.GetLoggerCtx(ctx, middlewareName, typeName)).
			Fatal(strings.Join([]string{"Replicate: failed to create a producer", err.Error()}, ": "))
		return
	}
	r.RWMutex.Lock()
	producer.SyncProducer(ctx)
	r.RWMutex.Unlock()
	r.producer = producer

	err = StartAlive(ctx, r.producer, middlewareName, config.Replicate.AliveTopic, time.Second*10)
	if err != nil {
		log.FromContext(middlewares.GetLoggerCtx(ctx, middlewareName, typeName)).
			Warn(strings.Join([]string{"Replicate: failed to start sending alive messages", err.Error()}, ": "))
	}
}

func sendEvent(ctx context.Context, producer Producer, event Event, name string) {
	logger := log.FromContext(middlewares.GetLoggerCtx(ctx, name, typeName))
	err := producer.Produce(event)
	if err != nil {
		logger.Error(err)
	}
}

func sendAlive(ctx context.Context, producer Producer, name string, topic string, duration time.Duration) {
	logger := log.FromContext(middlewares.GetLoggerCtx(ctx, name, typeName))
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
				logger.Debug(err)
			}
			err = producer.ProduceTo(Event{
				Host: hostname,
				Time: time.Now().UTC(),
			}, topic)
			if err != nil {
				logger.Debug(err)
			}
		}
	}
}
