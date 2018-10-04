package kafka

import (
	"log"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/go-kafkautils/consumer"
	"github.com/TerrexTech/go-kafkautils/producer"
)

type EventQuery struct {
	AggregateID      int
	AggregateVersion int
	YearBucket       int
}

func KafkaProducer(producerJSON string) {
	config := producer.Config{
		KafkaBrokers: []string{"kafka:9092"},
	}

	asyncProducer, err := producer.New(&config)
	if err != nil {
		panic(err)
	}
	asyncProducer.EnableLogging()

	go func() {
		for err := range asyncProducer.Errors() {
			log.Println(err)
		}
	}()

	input, err := asyncProducer.Input()
	if err != nil {
		log.Println(err)
	}
	input <- &sarama.ProducerMessage{
		Topic: "events.rns_eventstore.eventsquery",
		Value: sarama.StringEncoder(producerJSON),
	}

}

func KafkaConsumer(config consumer.Config) {

	proxyconsumer, err := consumer.New(&config)
	if err != nil {
		panic(err)
	}
	proxyconsumer.EnableLogging()

	// Read Errors
	go func() {
		for err := range proxyconsumer.Errors() {
			log.Println(err)
		}
	}()

	// Read Messages
	go func() {
		for msg := range proxyconsumer.Messages() {
			log.Println(msg)
		}
	}()
}

// type KafAdapter struct {
// 	Address         []string
// 	ResponseTopic   string
// 	ProducerErrChan <-chan *sarama.ProducerError
// 	ProducerResChan chan *model.KafkaResponse
// }

// func ResponseProducer(p KafAdapter) (*producer.Producer, error) {
// 	config := producer.Config{
// 		KafkaBrokers: p.Address,
// 	}
// 	asyncProducer, err := producer.New(&config)
// 	if err != nil {
// 		err = errors.Wrap(err, "Error while creating responses.")
// 		return nil, err
// 	}
// 	return asyncProducer, nil
// }

// func CreateProducer(adapter KafAdapter) (*KafAdapter, error) {

// 	resProducer, err := ResponseProducer(adapter)
// 	if err != nil {
// 		err = errors.Wrap(err, "Error while creating responses.")
// 		// return nil, err
// 	}

// 	responseProducerInput, err := resProducer.Input()
// 	if err != nil {
// 		err = errors.Wrap(err, "Error while creating responses")
// 		// return nil, err
// 	}

// 	// Setup Producer I/O channels
// 	// ProducerResChan := make(chan *model.KafkaResponse)
// 	// ProducerResChan := adapter.ProducerResChan
// 	kafkaIo := &KafAdapter{
// 		// ProducerResChan: (chan<- *model.KafkaResponse)(ProducerResChan),
// 		ProducerErrChan: resProducer.Errors(),
// 	}

// 	go func() {
// 		for message := range adapter.ProducerResChan {
// 			jsonMessage, err := json.Marshal(message)
// 			if err != nil {
// 				err = errors.Wrapf(err, "Error Marshalling KafkaResponse")
// 				log.Println(err)
// 			}

// 			topic := fmt.Sprintf("%s.%d", adapter.ResponseTopic, message.AggregateID)
// 			producerMessage := producer.CreateMessage(topic, jsonMessage)
// 			responseProducerInput <- producerMessage
// 		}
// 	}()
// 	return kafkaIo, nil
// }
