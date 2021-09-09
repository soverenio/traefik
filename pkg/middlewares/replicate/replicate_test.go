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
)

func TestReplicate(t *testing.T) {
	t.Run("The middleware doesn't affect on request and response", func(t *testing.T) {
		URL := "/test"
		method := http.MethodPost
		expectedBody := `{"key": "value"}`
		expectedHeader := "X-Header"
		expectedValue := "header value"
		expectedEvent := Event{
			Method: method,
			URL:    URL,
			Host:   "example.com",
			Client: "192.0.2.1:1234",
			Request: Payload{
				Body:    expectedBody,
				Headers: map[string][]string{},
			},
			Response: Payload{
				Body:    expectedBody,
				Headers: map[string][]string{expectedHeader: {expectedValue}},
			},
		}

		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body := r.Body
			defer body.Close()

			bytes, err := ioutil.ReadAll(body)
			require.NoError(t, err)
			assert.Equal(t, expectedBody, string(bytes), "request body was changed by the middleware")

			w.Header().Set(expectedHeader, expectedValue)
			_, err = w.Write([]byte(expectedBody))
			require.NoError(t, err)
		})

		producer := MockProducer(func(event Event) error {
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
			producer: producer,
			wPool:    newLimitPool(ctx, defaultPoolSize),
		}

		request := httptest.NewRequest(method, URL, strings.NewReader(expectedBody))
		recorder := httptest.NewRecorder()
		replicate.ServeHTTP(recorder, request)

		assert.Equal(t, http.StatusOK, recorder.Code, "status code was changed by the middleware")
		assert.Equal(t, expectedBody, recorder.Body.String(), "response body was changed by the middleware")
		assert.Len(t, recorder.Header(), 2, "length of headers was changed by the middleware")
		assert.Equal(t, recorder.Header().Get(expectedHeader), expectedValue, "header was changed by the middleware")
	})

	t.Run("Producer error causes, but handler return 200", func(t *testing.T) {
		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := w.Write([]byte("body"))
			require.NoError(t, err)
		})
		producer := MockProducer(func(event Event) error {
			return errors.New("test-error")
		})

		ctx := context.Background()
		replicate := replicate{
			RWMutex:  sync.RWMutex{},
			next:     next,
			name:     "test-replicate",
			producer: producer,
			wPool:    newLimitPool(ctx, defaultPoolSize),
		}
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/test", nil)
		replicate.ServeHTTP(recorder, request)

		assert.Equal(t, http.StatusOK, recorder.Code, "status code must be 200")
		assert.Equal(t, "body", recorder.Body.String(), "response body is correct")
	})
}

func TestAlive(t *testing.T) {
	t.Run("Alive message send", func(t *testing.T) {
		var calls int
		producer := MockProducer(func(event Event) error {
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

type MockProducer func(Event) error

func (m MockProducer) produce(ev Event) error {
	return m(ev)
}

func (m MockProducer) produceTo(ev Event, topic string) error {
	return m(ev)
}
