package replicate

import (
	"bytes"
	"context"
	"errors"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"github.com/traefik/traefik/v2/pkg/log"
	"github.com/traefik/traefik/v2/pkg/middlewares"

	"github.com/traefik/traefik/v2/pkg/safe"
)

const (
	typeName = "Replicate"
)

// replicate is a middleware used to send copies of requests and responses to an arbitrary service
type replicate struct {
	next     http.Handler
	name     string
	producer Producer
}

// New creates a new http handler.
func New(ctx context.Context, next http.Handler, producer Producer, name string) (http.Handler, error) {
	log.FromContext(middlewares.GetLoggerCtx(ctx, name, typeName)).Debug("Creating middleware")
	return &replicate{
		next:     next,
		name:     name,
		producer: producer,
	}, nil
}

func (r *replicate) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	logger := log.FromContext(middlewares.GetLoggerCtx(context.Background(), r.name, typeName))

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

	err = r.producer.Produce(Event{
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
	})
	if err != nil {
		logger.Debug(err)
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	_, err = rw.Write(responseBody)
	if err != nil {
		logger.Debug(err)
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
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
