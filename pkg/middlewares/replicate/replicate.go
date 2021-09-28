package replicate

import (
	"bytes"
	"context"
	"errors"
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
	typeName              = "Replicate"
	emptyJSONBody         = "{}"
	defaultMaxPayloadSize = 1000000
)

// replicate is a middleware used to send copies of requests and responses to an arbitrary service.
type replicate struct {
	sync.RWMutex
	next           http.Handler
	name           string
	producer       producer.Producer
	wPool          *utils.WorkerPool
	maxPayloadSize int
}

// New creates a new http handler.
func New(ctx context.Context, next http.Handler, config *runtime.MiddlewareInfo, middlewareName string) http.Handler {
	ctx = middlewares.GetLoggerCtx(ctx, middlewareName, typeName)
	log.FromContext(ctx).Debug("Creating middleware")

	maxPayloadSize := config.Replicate.MaxPayloadSize
	if maxPayloadSize == 0 {
		logger := log.FromContext(ctx)
		logger.Debugf("maxPayloadSize equal zero from config, use default max payload size %d", defaultMaxPayloadSize)
		maxPayloadSize = defaultMaxPayloadSize
	}

	replicate := &replicate{
		next:           next,
		name:           middlewareName,
		producer:       nil,
		wPool:          utils.NewLimitPool(ctx, config.Replicate.WorkerPoolSize),
		maxPayloadSize: maxPayloadSize,
	}
	replicate.wPool.Start()

	go replicate.connectProducer(ctx, config, middlewareName, next)
	return replicate
}

func (r *replicate) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	ctx := context.Background()
	logger := log.FromContext(middlewares.GetLoggerCtx(ctx, r.name, typeName))

	if req.ContentLength > int64(r.maxPayloadSize) {
		logger.Debugf("ignoring requests with too long body: body length is %d", req.ContentLength)
		r.next.ServeHTTP(rw, req)
		return
	}

	body := req.Body
	defer body.Close()

	method := req.Method
	URL := req.URL.String()
	host := req.Host

	remoteAddr, _, err := net.SplitHostPort(req.RemoteAddr)
	if err != nil {
		logger.Warn(err, "invalid remote address: ", req.RemoteAddr)
		remoteAddr = req.RemoteAddr
	}

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

	size := len(requestBody)
	if size > r.maxPayloadSize {
		logger.Debugf("ignoring requests with too long body: body length is %d", size)
		return
	}

	size = len(responseBody)
	if size > r.maxPayloadSize {
		logger.Debugf("ignoring responses with too long body: body length is %d", size)
		return
	}

	var eventRequest, eventResponse producer.Payload

	if ct := requestHeaders.Get("Content-Type"); strings.Contains(ct, "application/json") {
		eventRequest = producer.Payload{
			Body:    string(requestBody),
			Headers: requestHeaders,
		}
	} else {
		logger.Debug("ignoring requests with header 'Content-Type' not 'application/json', setting Event.Request to '{}'")
		eventRequest = producer.Payload{
			Body:    emptyJSONBody,
			Headers: map[string][]string{},
		}
	}

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
	if topic == "" {
		return errors.New("topic is required")
	}
	safe.Go(func() {
		sendAlive(ctx, producer, name, topic, duration)
	})
	return nil
}

func (r *replicate) connectProducer(ctx context.Context, config *runtime.MiddlewareInfo, middlewareName string, next http.Handler) {
	producerInstance, err := producer.NewKafkaPublisher(config.Replicate.Topic, config.Replicate.Brokers)
	if err != nil {
		log.FromContext(middlewares.GetLoggerCtx(ctx, middlewareName, typeName)).
			Fatal(strings.Join([]string{"Replicate: failed to create a producer", err.Error()}, ": "))
		return
	}
	producerInstance.Connect(ctx)

	r.RWMutex.Lock()
	r.producer = producerInstance
	r.RWMutex.Unlock()

	err = StartAlive(ctx, r.producer, middlewareName, config.Replicate.AliveTopic, time.Second*10)
	if err != nil {
		log.FromContext(middlewares.GetLoggerCtx(ctx, middlewareName, typeName)).
			Warn(strings.Join([]string{"Replicate: failed to start sending alive messages", err.Error()}, ": "))
	}
}

func sendEvent(ctx context.Context, producer producer.Producer, event producer.Event, name string) {
	logger := log.FromContext(middlewares.GetLoggerCtx(ctx, name, typeName))
	err := producer.Produce(event)
	if err != nil {
		logger.Warn(err)
	}
}

func sendAlive(ctx context.Context, p producer.Producer, name string, topic string, duration time.Duration) {
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
			err = p.ProduceTo(producer.Event{
				Host: hostname,
				Time: time.Now().UTC(),
			}, topic)
			if err != nil {
				logger.Warn(err)
			}
		}
	}
}

func isVerifyEvent(ev producer.Event) bool {
	return ev.Host != "" && ev.Client != ""
}
