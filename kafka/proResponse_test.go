package kafka

import (
	"encoding/json"
	"log"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-kafkautils/consumer"
	"github.com/TerrexTech/go-kafkautils/producer"
	"github.com/pkg/errors"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestBooks(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Books Suite")
}

var _ = Describe("Producer test - Kafka", func() {

	var (
		// kaAdapter     *KafAdapter
		address           string
		consumerGroupName string
		responseTopic     string
		responseConsumer  *consumer.Consumer
		testInput         chan<- *sarama.ProducerMessage
	)

	address = "kafka:9092"
	// responseTopic = "KafkaProducerTest"

	BeforeSuite(func() {
		// kaAdapter = &KafAdapter{
		// 	Address: []string{address},
		// 	// ProducerResChan: input,
		// 	// ResponseTopic:   responseTopic,
		// }

		// config := consumer.Config{
		// 	ConsumerGroup: "test",
		// 	KafkaBrokers:  []string{"localhost:9092"},
		// 	Topics:        []string{"test"},
		// }

		config := &producer.Config{
			KafkaBrokers: []string{address},
		}

		consumerTopic := "test.1"
		consumerGroupName = "Groot"
		responseTopic = "test.1"

		kafProduce, err := producer.New(config)
		Expect(err).ToNot(HaveOccurred())

		testInput, err = kafProduce.Input()
		Expect(err).ToNot(HaveOccurred())

		testInput <- producer.CreateMessage(consumerTopic, []byte("This is a test. Do I exist?"))
		log.Println("Produced mock-event on consumer-topic")
	})

	BeforeEach(func() {
		if responseConsumer == nil {
			consumerConfig := &consumer.Config{
				ConsumerGroup: consumerGroupName,
				KafkaBrokers:  []string{address},
				Topics:        []string{responseTopic},
			}
			var err error
			responseConsumer, err = consumer.New(consumerConfig)
			Expect(err).ToNot(HaveOccurred())
		}
	})

	It("No errors in response consumer", func() {
		go func() {
			defer GinkgoRecover()
			for consumerErr := range responseConsumer.Errors() {
				Expect(consumerErr).ToNot(HaveOccurred())
			}
		}()
	})

	It("Should receive response after successfully consuming", func(done Done) {
		log.Println("Checking if the Kafka response-topic received the event with timeout of 10 seconds")

		for message := range responseConsumer.Messages() {
			log.Println("++++++++++++++++++++++++")
			// Mark the message-offset since we do not want the
			// same message to appear again in later tests.
			responseConsumer.MarkOffset(message, "")
			// Context is added to this error (using errors.Wrap) later below
			err := responseConsumer.SaramaConsumerGroup().CommitOffsets()
			err = errors.Wrap(err, "Error Committing Offsets for message")
			Expect(err).ToNot(HaveOccurred())

			// Unmarshal the Kafka-Response
			log.Println("An Event was received.")
			response := &model.KafkaResponse{}
			err = json.Unmarshal(message.Value, response)

			Expect(err).ToNot(HaveOccurred())
			Expect(response.Error).To(BeEmpty())

			close(done)
		}
	}, 10)

	// _, err := CreateProducer(*kaAdapter)
	// log.Println("Testing - does this work")
	// go func() {
	// 	input <- &model.KafkaResponse{
	// 		AggregateID: 91,
	// 		Input:       "BOOOO",
	// 	}
	// }()

	// go func() {
	// 	for msg := proxyConsumer.Messages() {
	// 	  log.Println(msg)
	// 	}
	//   }()

	// Expect(err).ToNot(HaveOccurred())

	// It("Should receive an error", func() {
	// 	kaAdapter = &KafAdapter{
	// 		Address:         []string{""},
	// 		ProducerResChan: input,
	// 		ResponseTopic:   responseTopic,
	// 	}
	// })

})
