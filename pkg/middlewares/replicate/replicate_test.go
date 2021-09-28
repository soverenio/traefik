package replicate

import (
	"context"
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/traefik/traefik/v2/pkg/middlewares/replicate/producer"
	"github.com/traefik/traefik/v2/pkg/middlewares/replicate/utils"
)

func TestReplicate(t *testing.T) {
	t.Run("The middleware doesn't affect on request and response", func(t *testing.T) {
		URL := "/test"
		method := http.MethodPost
		expectedBody := `{"key": "value"}`

		headers := make(map[string][]string, 2)
		customHeaderKey := "X-Header"
		headers[customHeaderKey] = []string{"header value"}
		contentTypeHeaderKey := "Content-Type"
		headers[contentTypeHeaderKey] = []string{"application/json"}

		expectedEvent := producer.Event{
			Method: method,
			URL:    URL,
			Host:   "example.com",
			Client: "192.0.2.1",
			Request: producer.Payload{
				Body:    expectedBody,
				Headers: headers,
			},
			Response: producer.Payload{
				Body:    expectedBody,
				Headers: headers,
			},
		}

		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body := r.Body
			defer body.Close()

			bytes, err := ioutil.ReadAll(body)
			require.NoError(t, err)
			assert.Equal(t, expectedBody, string(bytes), "request body was changed by the middleware")

			w.Header().Set(customHeaderKey, headers[customHeaderKey][0])
			w.Header().Set(contentTypeHeaderKey, headers[contentTypeHeaderKey][0])
			_, err = w.Write([]byte(expectedBody))
			require.NoError(t, err)
		})

		mockedProducer := MockProducer(func(event producer.Event) error {
			assert.NotEmpty(t, event.Time)
			expectedEvent.Time = event.Time
			assert.Equal(t, expectedEvent, event)
			return nil
		})
		ctx := context.Background()
		replicate := replicate{
			RWMutex:  sync.RWMutex{},
			next:     next,
			name:     "test-replicate",
			producer: mockedProducer,
			wPool:    utils.NewLimitPool(ctx, utils.DefaultPoolSize),
		}
		replicate.wPool.Start()

		request := httptest.NewRequest(method, URL, strings.NewReader(expectedBody))
		request.Header = headers
		recorder := httptest.NewRecorder()
		replicate.ServeHTTP(recorder, request)

		assert.Equal(t, http.StatusOK, recorder.Code, "status code was changed by the middleware")
		assert.Equal(t, expectedBody, recorder.Body.String(), "response body was changed by the middleware")
		assert.Len(t, recorder.Header(), 2, "length of headers was changed by the middleware")
		assert.Equal(t, headers[customHeaderKey][0], recorder.Header().Get(customHeaderKey), "response header was changed by the middleware")
		assert.Equal(t, headers[contentTypeHeaderKey][0], recorder.Header().Get(contentTypeHeaderKey), "response header was changed by the middleware")
		// timeout is needed to properly process mockedProducer func
		time.Sleep(time.Second)
	})

	t.Run("Producer error causes, but handler return 200", func(t *testing.T) {
		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := w.Write([]byte("body"))
			require.NoError(t, err)
		})
		mockedProducer := MockProducer(func(event producer.Event) error {
			return errors.New("test-error")
		})

		ctx := context.Background()
		replicate := replicate{
			RWMutex:  sync.RWMutex{},
			next:     next,
			name:     "test-replicate",
			producer: mockedProducer,
			wPool:    utils.NewLimitPool(ctx, utils.DefaultPoolSize),
		}
		replicate.wPool.Start()

		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/test", nil)
		replicate.ServeHTTP(recorder, request)

		assert.Equal(t, http.StatusOK, recorder.Code, "status code must be 200")
		assert.Equal(t, "body", recorder.Body.String(), "response body is correct")
		// timeout is needed to properly process mockedProducer func
		time.Sleep(time.Second)
	})
}

func TestReplicate_skip_request(t *testing.T) {
	URL := "/test"
	method := http.MethodPost
	expectedBody := `{"key": "value"}`

	headersReq := make(map[string][]string, 2)
	customHeaderKey := "X-Header"
	headersReq[customHeaderKey] = []string{"header value"}

	headersResp := make(map[string][]string, 2)
	headersResp[customHeaderKey] = []string{"header value"}
	contentTypeHeaderKey := "Content-Type"
	headersResp[contentTypeHeaderKey] = []string{"application/json"}

	expectedEvent := producer.Event{
		Method: method,
		URL:    URL,
		Host:   "example.com",
		Client: "192.0.2.1",
		Request: producer.Payload{
			Body:    emptyJSONBody,
			Headers: map[string][]string{},
		},
		Response: producer.Payload{
			Body:    expectedBody,
			Headers: headersResp,
		},
	}

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := r.Body
		defer body.Close()

		bytes, err := ioutil.ReadAll(body)
		require.NoError(t, err)
		assert.Equal(t, expectedBody, string(bytes), "request body was changed by the middleware")

		w.Header().Set(contentTypeHeaderKey, headersResp[contentTypeHeaderKey][0])
		w.Header().Set(customHeaderKey, headersResp[customHeaderKey][0])
		_, err = w.Write([]byte(expectedBody))
		require.NoError(t, err)
	})

	mockedProducer := MockProducer(func(event producer.Event) error {
		require.NotEmpty(t, event.Time)
		expectedEvent.Time = event.Time
		require.Equal(t, expectedEvent, event)
		return nil
	})

	replicate := replicate{
		RWMutex:  sync.RWMutex{},
		next:     next,
		name:     "test-replicate",
		producer: mockedProducer,
		wPool:    utils.NewLimitPool(context.Background(), utils.DefaultPoolSize),
	}
	replicate.wPool.Start()

	request := httptest.NewRequest(method, URL, strings.NewReader(expectedBody))
	request.Header = headersReq
	recorder := httptest.NewRecorder()
	replicate.ServeHTTP(recorder, request)

	assert.Equal(t, http.StatusOK, recorder.Code, "status code was changed by the middleware")
	assert.Equal(t, expectedBody, recorder.Body.String(), "response body was changed by the middleware")
	assert.Len(t, recorder.Header(), 2, "length of headers was changed by the middleware")
	assert.Equal(t, headersResp[contentTypeHeaderKey][0], recorder.Header().Get(contentTypeHeaderKey), "response header was changed by the middleware")
	assert.Equal(t, headersResp[customHeaderKey][0], recorder.Header().Get(customHeaderKey), "response header was changed by the middleware")
	// timeout is needed to properly process mockedProducer func
	time.Sleep(time.Second)
}

func TestReplicate_skip_response(t *testing.T) {
	URL := "/test"
	method := http.MethodPost
	expectedBody := `{"key": "value"}`

	headersReq := make(map[string][]string, 2)
	customHeaderKey := "X-Header"
	headersReq[customHeaderKey] = []string{"header value"}
	contentTypeHeaderKey := "Content-Type"
	headersReq[contentTypeHeaderKey] = []string{"application/json"}

	headersResp := make(map[string][]string, 2)
	headersResp[customHeaderKey] = []string{"header value"}

	expectedEvent := producer.Event{
		Method: method,
		URL:    URL,
		Host:   "example.com",
		Client: "192.0.2.1",
		Request: producer.Payload{
			Body:    expectedBody,
			Headers: headersReq,
		},
		Response: producer.Payload{
			Body:    emptyJSONBody,
			Headers: map[string][]string{},
		},
	}

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := r.Body
		defer body.Close()

		bytes, err := ioutil.ReadAll(body)
		require.NoError(t, err)
		assert.Equal(t, expectedBody, string(bytes), "request body was changed by the middleware")

		w.Header().Set(customHeaderKey, headersResp[customHeaderKey][0])
		_, err = w.Write([]byte(expectedBody))
		require.NoError(t, err)
	})

	mockedProducer := MockProducer(func(event producer.Event) error {
		require.NotEmpty(t, event.Time)
		expectedEvent.Time = event.Time
		require.Equal(t, expectedEvent, event)
		return nil
	})

	replicate := replicate{
		RWMutex:  sync.RWMutex{},
		next:     next,
		name:     "test-replicate",
		producer: mockedProducer,
		wPool:    utils.NewLimitPool(context.Background(), utils.DefaultPoolSize),
	}
	replicate.wPool.Start()

	request := httptest.NewRequest(method, URL, strings.NewReader(expectedBody))
	request.Header = headersReq
	recorder := httptest.NewRecorder()
	replicate.ServeHTTP(recorder, request)

	assert.Equal(t, http.StatusOK, recorder.Code, "status code was changed by the middleware")
	assert.Equal(t, expectedBody, recorder.Body.String(), "response body was changed by the middleware")
	assert.Len(t, recorder.Header(), 2, "length of headers was changed by the middleware")
	assert.Equal(t, headersResp[customHeaderKey][0], recorder.Header().Get(customHeaderKey), "response header was changed by the middleware")
	assert.Contains(t, recorder.Header().Get(contentTypeHeaderKey), "text/plain", "response header was changed by the middleware")
	// timeout is needed to properly process mockedProducer func
	time.Sleep(time.Second)
}

func TestAlive(t *testing.T) {
	t.Run("Alive message send", func(t *testing.T) {
		var calls int
		producer := MockProducer(func(event producer.Event) error {
			assert.NotEmpty(t, event.Time)
			assert.NotEmpty(t, event.Host)
			calls++
			return nil
		})

		duration := time.Second * 3
		ctx, cancel := context.WithCancel(context.Background())
		err := StartAlive(ctx, producer, "test-replicate", "alive", duration)
		require.NoError(t, err)
		time.Sleep(duration + time.Second)
		assert.Equal(t, 1, calls)
		cancel()
		time.Sleep(duration + time.Second)
		assert.Equal(t, 1, calls)
	})
}

type MockProducer func(producer.Event) error

func (m MockProducer) Produce(ev producer.Event) error {
	return m(ev)
}

func (m MockProducer) ProduceTo(ev producer.Event, topic string) error {
	return m(ev)
}
