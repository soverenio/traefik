package replicate

import (
	"context"
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewReplicate(t *testing.T) {
	t.Run("Creates an instance with valid params", func(t *testing.T) {
		next := http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {})
		producer := MockProducer(func(event Event) error { return nil })
		name := "test-replicate"

		handler, err := New(context.Background(), next, producer, name)
		assert.NoError(t, err)
		assert.NotNil(t, handler)
	})
}

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
		replicate, err := New(context.Background(), next, producer, "test-replicate")
		require.NoError(t, err)

		request := httptest.NewRequest(method, URL, strings.NewReader(expectedBody))
		recorder := httptest.NewRecorder()
		replicate.ServeHTTP(recorder, request)

		assert.Equal(t, http.StatusOK, recorder.Code, "status code was changed by the middleware")
		assert.Equal(t, expectedBody, recorder.Body.String(), "response body was changed by the middleware")
		assert.Len(t, recorder.Header(), 2, "length of headers was changed by the middleware")
		assert.Equal(t, recorder.Header().Get(expectedHeader), expectedValue, "header was changed by the middleware")
	})

	t.Run("Producer error causes Internal Server Error", func(t *testing.T) {
		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := w.Write([]byte("body"))
			require.NoError(t, err)
		})
		producer := MockProducer(func(event Event) error {
			return errors.New("test-error")
		})

		replicate, err := New(context.Background(), next, producer, "test-replicate")
		require.NoError(t, err)

		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/test", nil)
		replicate.ServeHTTP(recorder, request)

		assert.Equal(t, http.StatusInternalServerError, recorder.Code, "status code must be 500")
		assert.Equal(t, "test-error\n", recorder.Body.String(), "response body must contain an error message")
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

func (m MockProducer) Produce(ev Event) error {
	return m(ev)
}

func (m MockProducer) ProduceTo(ev Event, topic string) error {
	return m(ev)
}