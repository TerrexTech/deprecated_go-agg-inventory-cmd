package kafka

import (
	"log"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/go-kafkautils/consumer"
	"github.com/pkg/errors"
)

type KafkaConAdapter struct {
	Address            []string
	ConsumerGroup      string
	ConsumerTopics     []string
	ConsumerErrChan    <-chan error
	ConsumerMsgChan    <-chan *sarama.ConsumerMessage
	ConsumerOffsetChan chan<- *sarama.ConsumerMessage
}

func NewKafkaConAdapter(c *KafkaConAdapter) (*consumer.Consumer, error) {
	config := &consumer.Config{
		ConsumerGroup: c.ConsumerGroup,
		KafkaBrokers:  c.Address,
		Topics:        c.ConsumerTopics,
	}
	return consumer.New(config)
}

func Consume(c *KafkaConAdapter) (*KafkaConAdapter, error) {
	consumerEvent, err := NewKafkaConAdapter(c)
	if err != nil {
		err = errors.Wrap(err, "Error Creating ConsumerGroup for Events")
		// return nil, err
	}

	// A channel which receives consumer-messages to be committed
	consumerOffsetChan := make(chan *sarama.ConsumerMessage)
	kafkaIo := &KafkaConAdapter{
		ConsumerOffsetChan: (chan<- *sarama.ConsumerMessage)(consumerOffsetChan),
	}

	go func() {
		for msg := range consumerOffsetChan {
			consumerEvent.MarkOffset(msg, "")
		}
	}()
	log.Println("Created Kafka Event Offset-Commit Channel")

	// Setup Consumer I/O channels
	kafkaIo = &KafkaConAdapter{
		ConsumerErrChan: consumerEvent.Errors(),
		ConsumerMsgChan: consumerEvent.Messages(),
	}
	log.Println("KafkaIO Ready")

	return kafkaIo, nil
}
