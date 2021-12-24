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
	t.Run("Producer error causes, but handler return 200", func(t *testing.T) {
		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := w.Write([]byte("body"))
			require.NoError(t, err)
		})
		wg := sync.WaitGroup{}
		wg.Add(1)
		mockedProducer := MockEventProducer(func(event producer.Event) error {
			wg.Done()
			return errors.New("test-error")
		})

		ctx := context.Background()
		replicate := replicate{
			RWMutex:                sync.RWMutex{},
			next:                   next,
			name:                   "test-replicate",
			producer:               mockedProducer,
			wPool:                  utils.NewLimitPool(ctx, utils.DefaultPoolSize),
			maxProcessableBodySize: defaultMaxProcessableBodySize,
			discardedRequests:      utils.NewSyncCounter(),
			failedRequests:         utils.NewSyncCounter(),
			successfulRequests:     utils.NewSyncCounter(),
		}
		replicate.wPool.Start()
		defer func() {
			wg.Wait()
			replicate.wPool.Stop()
			assert.EqualValues(t, 1, replicate.failedRequests.Load())
			assert.EqualValues(t, 0, replicate.discardedRequests.Load())
			assert.EqualValues(t, 0, replicate.successfulRequests.Load())
		}()

		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/test", nil)
		replicate.ServeHTTP(recorder, request)

		assert.Equal(t, http.StatusOK, recorder.Code, "status code must be 200")
		assert.Equal(t, "body", recorder.Body.String(), "response body is correct")
	})
}

func TestReplicate_content_type_header(t *testing.T) {
	t.Run("process request and response with types application/json", func(t *testing.T) {
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

		wg := sync.WaitGroup{}
		wg.Add(1)
		mockedProducer := MockEventProducer(func(event producer.Event) error {
			assert.NotEmpty(t, event.Time)
			expectedEvent.Time = event.Time
			assert.Equal(t, expectedEvent, event)
			wg.Done()
			return nil
		})
		ctx := context.Background()
		replicate := replicate{
			RWMutex:                sync.RWMutex{},
			next:                   next,
			name:                   "test-replicate",
			producer:               mockedProducer,
			wPool:                  utils.NewLimitPool(ctx, utils.DefaultPoolSize),
			maxProcessableBodySize: defaultMaxProcessableBodySize,
			discardedRequests:      utils.NewSyncCounter(),
			failedRequests:         utils.NewSyncCounter(),
			successfulRequests:     utils.NewSyncCounter(),
		}
		replicate.wPool.Start()
		defer func() {
			wg.Wait()
			replicate.wPool.Stop()
			assert.EqualValues(t, 0, replicate.failedRequests.Load())
			assert.EqualValues(t, 0, replicate.discardedRequests.Load())
			assert.EqualValues(t, 1, replicate.successfulRequests.Load())
		}()

		request := httptest.NewRequest(method, URL, strings.NewReader(expectedBody))
		request.Header = headers
		recorder := httptest.NewRecorder()
		replicate.ServeHTTP(recorder, request)

		assert.Equal(t, http.StatusOK, recorder.Code, "status code was changed by the middleware")
		assert.Equal(t, expectedBody, recorder.Body.String(), "response body was changed by the middleware")
		assert.Len(t, recorder.Header(), 2, "length of headers was changed by the middleware")
		assert.Equal(t, headers[customHeaderKey][0], recorder.Header().Get(customHeaderKey), "response header was changed by the middleware")
		assert.Equal(t, headers[contentTypeHeaderKey][0], recorder.Header().Get(contentTypeHeaderKey), "response header was changed by the middleware")
	})
	t.Run("skip request without required header", func(t *testing.T) {
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

		wg := sync.WaitGroup{}
		wg.Add(1)
		mockedProducer := MockEventProducer(func(event producer.Event) error {
			require.NotEmpty(t, event.Time)
			expectedEvent.Time = event.Time
			require.Equal(t, expectedEvent, event)
			wg.Done()
			return nil
		})

		replicate := replicate{
			RWMutex:                sync.RWMutex{},
			next:                   next,
			name:                   "test-replicate",
			producer:               mockedProducer,
			wPool:                  utils.NewLimitPool(context.Background(), utils.DefaultPoolSize),
			maxProcessableBodySize: defaultMaxProcessableBodySize,
			discardedRequests:      utils.NewSyncCounter(),
			failedRequests:         utils.NewSyncCounter(),
			successfulRequests:     utils.NewSyncCounter(),
		}
		replicate.wPool.Start()
		defer func() {
			wg.Wait()
			replicate.wPool.Stop()
			assert.EqualValues(t, 0, replicate.failedRequests.Load())
			assert.EqualValues(t, 0, replicate.discardedRequests.Load())
			assert.EqualValues(t, 1, replicate.successfulRequests.Load())
		}()

		request := httptest.NewRequest(method, URL, strings.NewReader(expectedBody))
		request.Header = headersReq
		recorder := httptest.NewRecorder()
		replicate.ServeHTTP(recorder, request)

		assert.Equal(t, http.StatusOK, recorder.Code, "status code was changed by the middleware")
		assert.Equal(t, expectedBody, recorder.Body.String(), "response body was changed by the middleware")
		assert.Len(t, recorder.Header(), 2, "length of headers was changed by the middleware")
		assert.Equal(t, headersResp[contentTypeHeaderKey][0], recorder.Header().Get(contentTypeHeaderKey), "response header was changed by the middleware")
		assert.Equal(t, headersResp[customHeaderKey][0], recorder.Header().Get(customHeaderKey), "response header was changed by the middleware")
	})
	t.Run("skip response without required header", func(t *testing.T) {
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

		wg := sync.WaitGroup{}
		wg.Add(1)
		mockedProducer := MockEventProducer(func(event producer.Event) error {
			require.NotEmpty(t, event.Time)
			expectedEvent.Time = event.Time
			require.Equal(t, expectedEvent, event)
			wg.Done()
			return nil
		})

		replicate := replicate{
			RWMutex:                sync.RWMutex{},
			next:                   next,
			name:                   "test-replicate",
			producer:               mockedProducer,
			wPool:                  utils.NewLimitPool(context.Background(), utils.DefaultPoolSize),
			maxProcessableBodySize: defaultMaxProcessableBodySize,
			discardedRequests:      utils.NewSyncCounter(),
			failedRequests:         utils.NewSyncCounter(),
			successfulRequests:     utils.NewSyncCounter(),
		}
		replicate.wPool.Start()
		defer func() {
			wg.Wait()
			replicate.wPool.Stop()
			assert.EqualValues(t, 0, replicate.failedRequests.Load())
			assert.EqualValues(t, 0, replicate.discardedRequests.Load())
			assert.EqualValues(t, 1, replicate.successfulRequests.Load())
		}()

		request := httptest.NewRequest(method, URL, strings.NewReader(expectedBody))
		request.Header = headersReq
		recorder := httptest.NewRecorder()
		replicate.ServeHTTP(recorder, request)

		assert.Equal(t, http.StatusOK, recorder.Code, "status code was changed by the middleware")
		assert.Equal(t, expectedBody, recorder.Body.String(), "response body was changed by the middleware")
		assert.Len(t, recorder.Header(), 2, "length of headers was changed by the middleware")
		assert.Equal(t, headersResp[customHeaderKey][0], recorder.Header().Get(customHeaderKey), "response header was changed by the middleware")
		assert.Contains(t, recorder.Header().Get(contentTypeHeaderKey), "text/plain", "response header was changed by the middleware")
	})
	t.Run("process request with type application/x-www-form-urlencoded", func(t *testing.T) {
		URL := "/test"
		method := http.MethodPost
		expectedBody := `{"key": "value"}`

		customHeaderKey := "X-Header"
		contentTypeHeaderKey := "Content-Type"
		headersReq := make(map[string][]string, 2)
		headersReq[customHeaderKey] = []string{"header value"}
		headersReq[contentTypeHeaderKey] = []string{"application/x-www-form-urlencoded"}

		headersResp := make(map[string][]string, 2)
		headersResp[customHeaderKey] = []string{"header value"}
		headersResp[contentTypeHeaderKey] = []string{"application/json"}

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

			w.Header().Set(customHeaderKey, headersResp[customHeaderKey][0])
			w.Header().Set(contentTypeHeaderKey, headersResp[contentTypeHeaderKey][0])
			_, err = w.Write([]byte(expectedBody))
			require.NoError(t, err)
		})

		wg := sync.WaitGroup{}
		wg.Add(1)
		mockedProducer := MockEventProducer(func(event producer.Event) error {
			assert.NotEmpty(t, event.Time)
			expectedEvent.Time = event.Time
			assert.Equal(t, expectedEvent, event)
			wg.Done()
			return nil
		})

		replicate := replicate{
			RWMutex:                sync.RWMutex{},
			next:                   next,
			name:                   "test-replicate",
			producer:               mockedProducer,
			wPool:                  utils.NewLimitPool(context.Background(), utils.DefaultPoolSize),
			maxProcessableBodySize: defaultMaxProcessableBodySize,
			discardedRequests:      utils.NewSyncCounter(),
			failedRequests:         utils.NewSyncCounter(),
			successfulRequests:     utils.NewSyncCounter(),
		}
		replicate.wPool.Start()
		defer func() {
			wg.Wait()
			replicate.wPool.Stop()
			assert.EqualValues(t, 0, replicate.failedRequests.Load())
			assert.EqualValues(t, 0, replicate.discardedRequests.Load())
			assert.EqualValues(t, 1, replicate.successfulRequests.Load())
		}()

		request := httptest.NewRequest(method, URL, strings.NewReader(expectedBody))
		request.Header = headersReq
		recorder := httptest.NewRecorder()
		replicate.ServeHTTP(recorder, request)

		assert.Equal(t, http.StatusOK, recorder.Code, "status code was changed by the middleware")
		assert.Equal(t, expectedBody, recorder.Body.String(), "response body was changed by the middleware")
		assert.Len(t, recorder.Header(), 2, "length of headers was changed by the middleware")
		assert.Equal(t, headersResp[customHeaderKey][0], recorder.Header().Get(customHeaderKey), "response header was changed by the middleware")
		assert.Contains(t, recorder.Header().Get(contentTypeHeaderKey), "application/json", "response header was changed by the middleware")
	})
}

func TestReplicate_too_long_body(t *testing.T) {
	t.Run("The middleware ignores requests with too long body", func(t *testing.T) {
		URL := "/test"
		method := http.MethodPost
		requestBody := "too long request body"
		maxProcessableBodySize := len(requestBody) - 1

		headers := make(map[string][]string, 2)
		customHeaderKey := "X-Header"
		headers[customHeaderKey] = []string{"header value"}
		contentTypeHeaderKey := "Content-Type"
		headers[contentTypeHeaderKey] = []string{"application/json"}

		expectedBody := "_"

		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body := r.Body
			defer body.Close()

			bytes, err := ioutil.ReadAll(body)
			require.NoError(t, err)
			assert.Equal(t, requestBody, string(bytes), "request body was changed by the middleware")

			w.Header().Set(contentTypeHeaderKey, headers[contentTypeHeaderKey][0])
			w.Header().Set(customHeaderKey, headers[customHeaderKey][0])
			_, err = w.Write([]byte(expectedBody))
			require.NoError(t, err)
		})

		mockedProducer := MockEventProducer(func(event producer.Event) error {
			assert.Fail(t, "producer should not be called")
			return nil
		})
		ctx := context.Background()
		replicate := replicate{
			RWMutex:                sync.RWMutex{},
			next:                   next,
			name:                   "test-replicate",
			producer:               mockedProducer,
			wPool:                  utils.NewLimitPool(ctx, utils.DefaultPoolSize),
			maxProcessableBodySize: maxProcessableBodySize,
			discardedRequests:      utils.NewSyncCounter(),
			failedRequests:         utils.NewSyncCounter(),
			successfulRequests:     utils.NewSyncCounter(),
		}
		replicate.wPool.Start()
		defer func() {
			replicate.wPool.Stop()
			assert.EqualValues(t, 0, replicate.failedRequests.Load())
			assert.EqualValues(t, 1, replicate.discardedRequests.Load())
			assert.EqualValues(t, 0, replicate.successfulRequests.Load())
		}()

		request := httptest.NewRequest(method, URL, strings.NewReader(requestBody))
		request.Header = headers
		recorder := httptest.NewRecorder()
		replicate.ServeHTTP(recorder, request)

		assert.Equal(t, http.StatusOK, recorder.Code, "status code was changed by the middleware")
		assert.Equal(t, expectedBody, recorder.Body.String(), "response body was changed by the middleware")
		assert.Len(t, recorder.Header(), 2, "length of headers was changed by the middleware")
		assert.Equal(t, headers[customHeaderKey][0], recorder.Header().Get(customHeaderKey), "response header was changed by the middleware")
		assert.Equal(t, headers[contentTypeHeaderKey][0], recorder.Header().Get(contentTypeHeaderKey), "response header was changed by the middleware")
	})
	t.Run("The middleware ignores responses with too long body", func(t *testing.T) {
		URL := "/test"
		method := http.MethodPost
		expectedBody := "too long response body"
		maxProcessableBodySize := len(expectedBody) - 1
		headers := make(map[string][]string, 2)
		customHeaderKey := "X-Header"
		headers[customHeaderKey] = []string{"header value"}
		contentTypeHeaderKey := "Content-Type"
		headers[contentTypeHeaderKey] = []string{"application/json"}

		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set(contentTypeHeaderKey, headers[contentTypeHeaderKey][0])
			w.Header().Set(customHeaderKey, headers[customHeaderKey][0])
			_, err := w.Write([]byte(expectedBody))
			require.NoError(t, err)
		})

		mockedProducer := MockEventProducer(func(event producer.Event) error {
			assert.Fail(t, "producer should not be called")
			return nil
		})
		ctx := context.Background()
		replicate := replicate{
			RWMutex:                sync.RWMutex{},
			next:                   next,
			name:                   "test-replicate",
			producer:               mockedProducer,
			wPool:                  utils.NewLimitPool(ctx, utils.DefaultPoolSize),
			maxProcessableBodySize: maxProcessableBodySize,
			discardedRequests:      utils.NewSyncCounter(),
			failedRequests:         utils.NewSyncCounter(),
			successfulRequests:     utils.NewSyncCounter(),
		}
		replicate.wPool.Start()
		defer func() {
			replicate.wPool.Stop()
			assert.EqualValues(t, 0, replicate.failedRequests.Load())
			assert.EqualValues(t, 1, replicate.discardedRequests.Load())
			assert.EqualValues(t, 0, replicate.successfulRequests.Load())
		}()

		request := httptest.NewRequest(method, URL, strings.NewReader(""))
		request.Header = headers
		recorder := httptest.NewRecorder()
		replicate.ServeHTTP(recorder, request)

		assert.Equal(t, http.StatusOK, recorder.Code, "status code was changed by the middleware")
		assert.Equal(t, expectedBody, recorder.Body.String(), "response body was changed by the middleware")
		assert.Len(t, recorder.Header(), 2, "length of headers was changed by the middleware")
		assert.Equal(t, headers[customHeaderKey][0], recorder.Header().Get(customHeaderKey), "response header was changed by the middleware")
		assert.Equal(t, headers[contentTypeHeaderKey][0], recorder.Header().Get(contentTypeHeaderKey), "response header was changed by the middleware")
	})
	t.Run("The middleware ignores too long total body", func(t *testing.T) {
		URL := "/test"
		method := http.MethodPost
		expectedBody := "part of too long body"
		maxProcessableBodySize := len(expectedBody)*2 - 1
		headers := make(map[string][]string, 2)
		customHeaderKey := "X-Header"
		headers[customHeaderKey] = []string{"header value"}
		contentTypeHeaderKey := "Content-Type"
		headers[contentTypeHeaderKey] = []string{"application/json"}

		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set(contentTypeHeaderKey, headers[contentTypeHeaderKey][0])
			w.Header().Set(customHeaderKey, headers[customHeaderKey][0])
			_, err := w.Write([]byte(expectedBody))
			require.NoError(t, err)
		})

		mockedProducer := MockEventProducer(func(event producer.Event) error {
			assert.Fail(t, "producer should not be called")
			return nil
		})
		ctx := context.Background()
		replicate := replicate{
			RWMutex:                sync.RWMutex{},
			next:                   next,
			name:                   "test-replicate",
			producer:               mockedProducer,
			wPool:                  utils.NewLimitPool(ctx, utils.DefaultPoolSize),
			maxProcessableBodySize: maxProcessableBodySize,
			discardedRequests:      utils.NewSyncCounter(),
			failedRequests:         utils.NewSyncCounter(),
			successfulRequests:     utils.NewSyncCounter(),
		}
		replicate.wPool.Start()
		defer func() {
			replicate.wPool.Stop()
			assert.EqualValues(t, 0, replicate.failedRequests.Load())
			assert.EqualValues(t, 1, replicate.discardedRequests.Load())
			assert.EqualValues(t, 0, replicate.successfulRequests.Load())
		}()

		request := httptest.NewRequest(method, URL, strings.NewReader(expectedBody))
		request.Header = headers
		recorder := httptest.NewRecorder()
		replicate.ServeHTTP(recorder, request)

		assert.Equal(t, http.StatusOK, recorder.Code, "status code was changed by the middleware")
		assert.Equal(t, expectedBody, recorder.Body.String(), "response body was changed by the middleware")
		assert.Len(t, recorder.Header(), 2, "length of headers was changed by the middleware")
		assert.Equal(t, headers[customHeaderKey][0], recorder.Header().Get(customHeaderKey), "response header was changed by the middleware")
		assert.Equal(t, headers[contentTypeHeaderKey][0], recorder.Header().Get(contentTypeHeaderKey), "response header was changed by the middleware")
	})
}

func TestReplicate_not_enough_workers(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})

	wg := sync.WaitGroup{}
	wg.Add(1)
	sleep := make(chan struct{})
	mockedProducer := MockEventProducer(func(event producer.Event) error {
		if event.URL == "/test" {
			wg.Done()
		}
		<-sleep
		return nil
	})

	replicate := replicate{
		RWMutex:                sync.RWMutex{},
		next:                   next,
		name:                   "test-replicate",
		producer:               mockedProducer,
		wPool:                  utils.NewLimitPool(context.Background(), 1),
		maxProcessableBodySize: defaultMaxProcessableBodySize,
		discardedRequests:      utils.NewSyncCounter(),
		failedRequests:         utils.NewSyncCounter(),
		successfulRequests:     utils.NewSyncCounter(),
	}
	replicate.wPool.Start()
	defer func() {
		replicate.wPool.Stop()
		assert.EqualValues(t, 1, replicate.wPool.LoadDiscarded())
		assert.EqualValues(t, 0, replicate.discardedRequests.Load())
		assert.EqualValues(t, 0, replicate.failedRequests.Load())
		assert.EqualValues(t, 2, replicate.successfulRequests.Load())
	}()

	recorder := httptest.NewRecorder()
	// send 3 request, pool has only one worker, queue length is one
	replicate.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/test", nil))
	wg.Wait()
	replicate.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/queue", nil))
	replicate.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/discarded", nil))
	close(sleep)
}

func TestHeartbeat(t *testing.T) {
	t.Run("Heartbeat message send", func(t *testing.T) {
		var calls int

		expectedPoolDiscarded := uint64(5)
		expectedDiscarded := uint64(10)
		expectedFailed := uint64(20)
		expectedSuccessful := uint64(50)
		producer := MockHeartbeatProducer(func(heartbeat producer.Heartbeat) error {
			assert.NotEmpty(t, heartbeat.Time)
			assert.NotEmpty(t, heartbeat.Host)
			assert.EqualValues(t, expectedPoolDiscarded+expectedDiscarded, heartbeat.Discarded)
			assert.EqualValues(t, expectedFailed, heartbeat.Failed)
			assert.EqualValues(t, expectedSuccessful, heartbeat.Successful)
			calls++
			return nil
		})

		duration := time.Second * 3
		ctx, cancel := context.WithCancel(context.Background())
		replicate := replicate{
			RWMutex:            sync.RWMutex{},
			name:               "test-replicate",
			producer:           producer,
			wPool:              utils.NewLimitPool(ctx, utils.DefaultPoolSize),
			discardedRequests:  utils.NewSyncCounter(),
			failedRequests:     utils.NewSyncCounter(),
			successfulRequests: utils.NewSyncCounter(),
		}
		replicate.wPool.AddDiscarded(expectedPoolDiscarded)
		replicate.discardedRequests.Add(expectedDiscarded)
		replicate.failedRequests.Add(expectedFailed)
		replicate.successfulRequests.Add(expectedSuccessful)

		err := replicate.StartHeartbeat(ctx, producer, "test-replicate", duration)
		require.NoError(t, err)
		time.Sleep(duration + time.Second)
		assert.Equal(t, 1, calls)
		cancel()
		time.Sleep(duration + time.Second)
		assert.Equal(t, 1, calls)

		assert.EqualValues(t, 0, replicate.wPool.LoadDiscarded())
		assert.EqualValues(t, 0, replicate.discardedRequests.Load())
		assert.EqualValues(t, 0, replicate.failedRequests.Load())
		assert.EqualValues(t, 0, replicate.successfulRequests.Load())
	})
	t.Run("Heartbeat message send error", func(t *testing.T) {
		var calls int

		producer := MockHeartbeatProducer(func(heartbeat producer.Heartbeat) error {
			calls++
			return errors.New("test-error")
		})

		duration := time.Second * 3
		ctx, cancel := context.WithCancel(context.Background())
		replicate := replicate{
			RWMutex:            sync.RWMutex{},
			name:               "test-replicate",
			producer:           producer,
			wPool:              utils.NewLimitPool(ctx, utils.DefaultPoolSize),
			discardedRequests:  utils.NewSyncCounter(),
			failedRequests:     utils.NewSyncCounter(),
			successfulRequests: utils.NewSyncCounter(),
		}

		expectedPoolDiscarded := uint64(5)
		expectedDiscarded := uint64(10)
		expectedFailed := uint64(20)
		expectedSuccessful := uint64(50)

		replicate.wPool.AddDiscarded(expectedPoolDiscarded)
		replicate.discardedRequests.Add(expectedDiscarded)
		replicate.failedRequests.Add(expectedFailed)
		replicate.successfulRequests.Add(expectedSuccessful)

		err := replicate.StartHeartbeat(ctx, producer, "test-replicate", duration)
		require.NoError(t, err)
		time.Sleep(duration + time.Second)
		assert.Equal(t, 1, calls)
		cancel()

		assert.EqualValues(t, expectedPoolDiscarded, replicate.wPool.LoadDiscarded())
		assert.EqualValues(t, expectedDiscarded, replicate.discardedRequests.Load())
		assert.EqualValues(t, expectedFailed, replicate.failedRequests.Load())
		assert.EqualValues(t, expectedSuccessful, replicate.successfulRequests.Load())
	})
}

type MockEventProducer func(producer.Event) error

func (m MockEventProducer) ProduceEvent(ev producer.Event) error {
	return m(ev)
}

func (m MockEventProducer) ProduceHeartbeat(ev producer.Heartbeat) error {
	panic("illegal call")
}

type MockHeartbeatProducer func(producer.Heartbeat) error

func (m MockHeartbeatProducer) ProduceEvent(ev producer.Event) error {
	panic("illegal call")
}

func (m MockHeartbeatProducer) ProduceHeartbeat(hb producer.Heartbeat) error {
	return m(hb)
}
