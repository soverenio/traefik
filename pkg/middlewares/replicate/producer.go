package replicate

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

type Event struct {
	Method   string    `json:"method"`
	URL      string    `json:"url"`
	Host     string    `json:"host"`
	Client   string    `json:"client"`
	Request  Payload   `json:"request"`
	Response Payload   `json:"response"`
	Time     time.Time `json:"time"`
}

type Payload struct {
	Body    string              `json:"body"`
	Headers map[string][]string `json:"headers"`
}

type Producer interface {
	Produce(event Event) error
	ProduceTo(event Event, topic string) error
}

type KafkaPublisher struct {
	message.Publisher
	brokers []string
	Topic   string
}

// NewKafkaPublisher create new  KafkaPublisher
func NewKafkaPublisher(topic string, brokers []string) (*KafkaPublisher, error) {
	if topic == "" {
		return nil, errors.New("topic is required")
	}
	if len(brokers) == 0 {
		return nil, errors.New("at least one broker is required")
	}

	return &KafkaPublisher{
		Publisher: nil,
		Topic:     topic,
		brokers:   brokers,
	}, nil
}

// SyncProducer connect to kafka
func (p *KafkaPublisher) SyncProducer(ctx context.Context) {
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
			publisher, err := kafka.NewPublisher(config, watermill.NewStdLogger(true, false))
			if err == nil {
				p.Publisher = publisher
				return
			}
			logger.Error("failed to create a producer")
		}
	}
}

// Produce send event to kafka
func (p *KafkaPublisher) Produce(ev Event) error {
	logger := log.FromContext(context.Background())
	payload, err := json.Marshal(ev)
	if err != nil {
		logger.Error(err)
		return err
	}
	err = p.Publish(p.Topic, message.NewMessage(watermill.NewUUID(), payload))
	if err != nil {
		logger.Error(err)
		return err
	}
	logger.Debug("Send message to kafka")

	return nil
}

// ProduceTo send event to kafka in specific topic
func (p *KafkaPublisher) ProduceTo(ev Event, topic string) error {
	if topic == "" {
		return errors.New("topic is required")
	}
	payload, err := json.Marshal(ev)
	if err != nil {
		return err
	}

	return p.Publish(topic, message.NewMessage(watermill.NewUUID(), payload))
}
