package producer

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/sirupsen/logrus"
	"github.com/traefik/traefik/v2/pkg/log"
)

type watermillLogger struct {
	Log log.Logger
}

func (w *watermillLogger) Error(msg string, err error, fields watermill.LogFields) {
	fields = fields.Add(watermill.LogFields{"err": err.Error()})
	w.Log.WithFields(logrusFields(fields)).Error(msg)
}

func (w *watermillLogger) Info(msg string, fields watermill.LogFields) {
	w.Log.WithFields(logrusFields(fields)).Info(msg)
}

func (w *watermillLogger) Debug(msg string, fields watermill.LogFields) {
	w.Log.WithFields(logrusFields(fields)).Debug(msg)
}

func (w *watermillLogger) Trace(msg string, fields watermill.LogFields) {
	w.Log.WithFields(logrusFields(fields)).Debug(msg)
}

func (w *watermillLogger) With(fields watermill.LogFields) watermill.LoggerAdapter {
	return &watermillLogger{w.Log.WithFields(logrusFields(fields))}
}

func logrusFields(wtFields watermill.LogFields) logrus.Fields {
	fields := make(logrus.Fields)
	ff := map[string]interface{}(wtFields)
	for key, value := range ff {
		fields[key] = value
	}
	return fields
}
