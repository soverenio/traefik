package replicate

import (
	"context"
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewReplicate(t *testing.T) {
	t.Run("Creates instance with valid params", func(t *testing.T) {
		next := http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {})
		producer := MockProducer(func(event Event) error { return nil })
		name := "test-replicate"

		handler, err := New(context.Background(), next, producer, name)
		assert.NoError(t, err)
		assert.NotNil(t, handler)
	})
}

func TestReplicate(t *testing.T) {
	t.Run("Don't affect on request and response", func(t *testing.T) {
		URL := "/test"
		method := http.MethodPost
		expectedBody := `{"key": "value"}`
		expectedHeader := "X-Header"
		expectedValue := "header value"
		expectedEvent := Event{
			Method:       method,
			URL:          URL,
			RequestBody:  expectedBody,
			ResponseBody: expectedBody,
		}

		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body := r.Body
			defer body.Close()

			bytes, err := ioutil.ReadAll(body)
			require.NoError(t, err)
			assert.Equal(t, expectedBody, string(bytes), "request body was changed by middleware")

			w.Header().Set(expectedHeader, expectedValue)
			_, err = w.Write([]byte(expectedBody))
			require.NoError(t, err)
		})

		producer := MockProducer(func(event Event) error {
			assert.Equal(t, expectedEvent, event)
			return nil
		})
		replicate, err := New(context.Background(), next, producer, "test-replicate")
		require.NoError(t, err)

		request := httptest.NewRequest(method, URL, strings.NewReader(expectedBody))
		recorder := httptest.NewRecorder()
		replicate.ServeHTTP(recorder, request)

		assert.Equal(t, http.StatusOK, recorder.Code, "status code was changed by middleware")
		assert.Equal(t, expectedBody, recorder.Body.String(), "response body was changed by middleware")
		assert.Len(t, recorder.Header(), 2, "length of headers was changed by middleware")
		assert.Equal(t, recorder.Header().Get(expectedHeader), expectedValue, "header was changed by middleware")
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

type MockProducer func(Event) error

func (m MockProducer) Produce(ev Event) error {
	return m(ev)
}
