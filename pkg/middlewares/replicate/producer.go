package replicate

import (
	"encoding/json"
	"errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
)

type Event struct {
	Method       string `json:"method"`
	URL          string `json:"url"`
	RequestBody  string `json:"requestBody"`
	ResponseBody string `json:"responseBody"`
}

type Producer interface {
	Produce(Event) error
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
