package producer

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/traefik/traefik/v2/pkg/log"
)

// Event is message with info about request and response.This message send to kafka.
type Event struct {
	Method       string    `json:"method"`
	URL          string    `json:"url"`
	Host         string    `json:"host"`
	Client       string    `json:"client"`
	Request      Payload   `json:"request"`
	Response     Payload   `json:"response"`
	ResponseCode int       `json:"code"`
	Time         time.Time `json:"time"`
}

// Heartbeat is message with info about replicate middleware status. This message send to kafka.
type Heartbeat struct {
	Host         string    `json:"host"`
	Time         time.Time `json:"time"`
	Discarded    uint64    `json:"discarded"`
	Failed       uint64    `json:"failed"`
	Successful   uint64    `json:"successful"`
	BuildVersion string    `json:"build_version"`
}

// Payload body and headers of  request and response.
type Payload struct {
	Body    string              `json:"body"`
	Headers map[string][]string `json:"headers"`
}

// Producer is interface for send message in message brokers.
type Producer interface {
	ProduceEvent(event Event) error
	ProduceHeartbeat(event Heartbeat) error
}

// KafkaPublisher publisher for kafka.
type KafkaPublisher struct {
	message.Publisher
	brokers        []string
	topic          string
	heartbeatTopic string
}

// NewKafkaPublisher create new KafkaPublisher.
func NewKafkaPublisher(topic, heartbeatTopic string, brokers []string) (*KafkaPublisher, error) {
	if topic == "" {
		return nil, errors.New("topic is required")
	}
	if heartbeatTopic == "" {
		return nil, errors.New("heartbeat topic is required")
	}
	if len(brokers) == 0 {
		return nil, errors.New("at least one broker is required")
	}

	return &KafkaPublisher{
		Publisher:      nil,
		topic:          topic,
		heartbeatTopic: heartbeatTopic,
		brokers:        brokers,
	}, nil
}

// Connect to kafka.
func (p *KafkaPublisher) Connect(ctx context.Context) {
	ctx = log.With(ctx, log.Str("component", "kafkaPublisher"))
	logger := log.FromContext(ctx)

	config := kafka.PublisherConfig{
		Brokers:   p.brokers,
		Marshaler: kafka.DefaultMarshaler{},
	}
	for {
		select {
		case <-ctx.Done():
			logger.Info("completing the attempt to connect to kafka")
			return
		default:
			publisher, err := kafka.NewPublisher(config, &watermillLogger{Log: logger.WithField("service", "watermill")})
			if err == nil {
				logger.Info("kafka publisher created")
				p.Publisher = publisher
				return
			}
			logger.Warnf("error while creating a new publisher instance: %v", err)
		}
	}
}

// ProduceEvent send event to kafka.
func (p *KafkaPublisher) ProduceEvent(ev Event) error {
	return p.produceTo(ev, p.topic)
}

// ProduceHeartbeat send heartbeat to kafka.
func (p *KafkaPublisher) ProduceHeartbeat(ev Heartbeat) error {
	return p.produceTo(ev, p.heartbeatTopic)
}

// ProduceTo send event to kafka in specific topic.
func (p *KafkaPublisher) produceTo(ev interface{}, topic string) error {
	if topic == "" {
		return errors.New("topic is required")
	}
	payload, err := json.Marshal(ev)
	if err != nil {
		return err
	}

	uuid := watermill.NewUUID()
	err = p.Publish(topic, message.NewMessage(uuid, payload))
	if err != nil {
		return err
	}

	return nil
}
