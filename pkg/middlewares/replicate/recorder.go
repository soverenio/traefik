package replicate

import (
	"bytes"
	"net/http"
)

type recorder interface {
	http.ResponseWriter
	GetBody() *bytes.Buffer
	GetStatusCode() int
}

func newResponseRecorder(rw http.ResponseWriter) recorder {
	return &responseRecorder{
		ResponseWriter: rw,
		body:           new(bytes.Buffer),
	}
}

type responseRecorder struct {
	http.ResponseWriter
	body       *bytes.Buffer
	statusCode int
}

func (r responseRecorder) Write(bytes []byte) (int, error) {
	return r.body.Write(bytes)
}

func (r *responseRecorder) GetBody() *bytes.Buffer {
	return r.body
}

func (r *responseRecorder) WriteHeader(statusCode int) {
	r.statusCode = statusCode
	r.ResponseWriter.WriteHeader(statusCode)
}

func (r *responseRecorder) GetStatusCode() int {
	return r.statusCode
}
