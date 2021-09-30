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
	Method   string    `json:"method"`
	URL      string    `json:"url"`
	Host     string    `json:"host"`
	Client   string    `json:"client"`
	Request  Payload   `json:"request"`
	Response Payload   `json:"response"`
	Time     time.Time `json:"time"`
}

// Payload body and headers of  request and response.
type Payload struct {
	Body    string              `json:"body"`
	Headers map[string][]string `json:"headers"`
}

// Producer is interface for send message in message brokers.
type Producer interface {
	Produce(event Event) error
	ProduceTo(event Event, topic string) error
}

// KafkaPublisher publisher for kafka.
type KafkaPublisher struct {
	message.Publisher
	brokers []string
	topic   string
}

// NewKafkaPublisher create new KafkaPublisher.
func NewKafkaPublisher(topic string, brokers []string) (*KafkaPublisher, error) {
	if topic == "" {
		return nil, errors.New("topic is required")
	}
	if len(brokers) == 0 {
		return nil, errors.New("at least one broker is required")
	}

	return &KafkaPublisher{
		Publisher: nil,
		topic:     topic,
		brokers:   brokers,
	}, nil
}

// Connect to kafka.
func (p *KafkaPublisher) Connect(ctx context.Context) {
	ctx = log.With(ctx, log.Str("component", "kafkaProducer"))
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
				logger.Debug("kafka publisher created")
				p.Publisher = publisher
				return
			}
			logger.Warnf("failed to create kafka publisher: %v", err)
		}
	}
}

// Produce send event to kafka.
func (p *KafkaPublisher) Produce(ev Event) error {
	return p.ProduceTo(ev, p.topic)
}

// ProduceTo send event to kafka in specific topic.
func (p *KafkaPublisher) ProduceTo(ev Event, topic string) error {
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
