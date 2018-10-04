package kafka

type ConsumerConfig struct {
	ConsumerGroup string
	KafkaBrokers  string
	Topics        string
}

type ProducerConfig struct {
	KafkaBrokers []string
}

// func GetInventory(inventory map[string]interface{}) {
// 	inventoryJSON, err := json.Marshal(inventory)
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to marshal inventory into JSON")
// 		return nil, err
// 	}

// }
