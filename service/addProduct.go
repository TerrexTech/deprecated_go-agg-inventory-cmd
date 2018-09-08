package service

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/bhupeshbhatia/go-agg-inventory-cmd/model"
	mgo "github.com/mongodb/mongo-go-driver/mongo"
	"github.com/pkg/errors"
)

var FoodProduct = `{
	"fruit_id": 4,
	"name": "Granny Smith Apples",
	"origin":"ON, Canada",
	"date_arrived": 1536362257717,
	"sale_price": 1.12,
	"original_weight": 700,
	"device_id": 1111
  }`

//Global variable = aggregate version and ID
var AggregateVersion = int64(1)
var AggregateID = int64(1)

func AddFoodItem(foodProduct []byte) (*mgo.InsertOneResult, error) {
	var inventory model.Inventory
	err := json.Unmarshal(foodProduct, &inventory)
	if err != nil {
		err = errors.Wrap(err, "Unable to unmarshal foodItem into Inventory struct")
		log.Println(err)
		return nil, err
	}

	fmt.Println(inventory)

	// //taking care of time
	// inventory.DateArrived = time.Now()

	// //Adding aggreate version and ID to inventory
	// inventory.AggregateVersion = AggregateVersion
	// inventory.AggregateID = AggregateID

	// mgTable, err := events.InitMongo()
	// if err != nil {
	// 	err = errors.Wrap(err, "Unable to connect to mongo")
	// 	log.Println(err)
	// 	// return nil, err
	// }

	// insertResult, err := mgTable.InsertOne(inventory)
	// if err != nil {
	// 	err = errors.Wrap(err, "Unable to insert event")
	// 	log.Println(err)
	// 	return nil, err
	// }

	// fmt.Println(insertResult)
	// return insertResult, nil
	return nil, nil
}

// type JsonTime time.Time

// type Time struct {
// 	Time time.Time
// }
