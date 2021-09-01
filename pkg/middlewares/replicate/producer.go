package replicate

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
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

	Topic string
}

func NewKafkaPublisher(topic string, brokers []string) (*KafkaPublisher, error) {
	logger := watermill.NewStdLogger(true, false)

	if topic == "" {
		return nil, errors.New("topic is required")
	}
	if len(brokers) == 0 {
		return nil, errors.New("at least one broker is required")
	}

	config := kafka.PublisherConfig{
		Brokers:   brokers,
		Marshaler: kafka.DefaultMarshaler{},
	}

	publisher, err := kafka.NewPublisher(config, logger)
	if err != nil {
		return nil, err
	}
	return &KafkaPublisher{
		Publisher: publisher,
		Topic:     topic,
	}, nil
}

func (p *KafkaPublisher) Produce(ev Event) error {
	payload, err := json.Marshal(ev)
	if err != nil {
		return err
	}

	return p.Publish(p.Topic, message.NewMessage(watermill.NewUUID(), payload))
}

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
