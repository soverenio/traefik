package producer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewKafkaPublisher(t *testing.T) {
	testCases := []struct {
		name    string
		topic   string
		brokers []string
		expMsg  string
	}{
		{
			name:    "Fails with empty topic",
			topic:   "",
			brokers: []string{""},
			expMsg:  "topic is required",
		},
		{
			name:    "Fails with empty brokers",
			topic:   "test-topic",
			brokers: []string{},
			expMsg:  "at least one broker is required",
		},
		{
			name:    "Fails with nil brokers",
			topic:   "test-topic",
			brokers: nil,
			expMsg:  "at least one broker is required",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			publisher, err := NewKafkaPublisher(tc.topic, tc.brokers)

			assert.Error(t, err)
			assert.Nil(t, publisher)
			assert.Contains(t, err.Error(), tc.expMsg)
		})
	}
}
