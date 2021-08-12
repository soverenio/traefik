package replicate

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"

	"github.com/traefik/traefik/v2/pkg/log"
	"github.com/traefik/traefik/v2/pkg/middlewares"
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

	err = r.producer.Produce(Event{
		Method:       method,
		URL:          URL,
		RequestBody:  string(requestBody),
		ResponseBody: string(responseBody),
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
