package events

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/bhupeshbhatia/go-agg-inventory-cmd/model"
	"github.com/pkg/errors"

	"github.com/TerrexTech/go-mongoutils/mongo"

	mgo "github.com/mongodb/mongo-go-driver/mongo"

	"github.com/bhupeshbhatia/go-agg-inventory-cmd/db"
)

// func ErrorStackTrace(err error) string {
// 	return fmt.Sprintf("%+v\n", err)
// }

func InitMongo() (*mongo.Collection, error) {
	hosts := os.Getenv("MONGO_HOSTS")
	username := os.Getenv("MONGO_USERNAME")
	password := os.Getenv("MONGO_PASSWORD")
	database := os.Getenv("MONGO_DATABASE")
	collection := os.Getenv("MONGO_COLLECTION")

	connStr := fmt.Sprintf("mongodb://%s:%s@%s", username, password, hosts)
	client, err := mgo.NewClient(connStr)
	if err != nil {
		err = errors.Wrap(err, "Mongo client not available")
		return nil, err
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Duration(1)*time.Second,
	)
	defer cancel()
	err = client.Connect(ctx)
	if err != nil {
		err = errors.Wrap(err, "Unable to connect to Mongo")
		return nil, err
	}

	config := &db.MongoConfig{
		Client:     client,
		TimeoutMS:  1000,
		Database:   database,
		Collection: collection,
	}
	return db.MongoCollection(config)
}

func InsertMockMongo(mgTable *mongo.Collection) (*mongo.Collection, error) {
	mockInventory := model.Inventory{
		FruitID:      1,
		RsCustomerID: "2",
		// Name:         "Test",
		DateBought:       time.Now(),
		DateSold:         time.Now().Add(2),
		SalePrice:        3.00,
		OriginalWeight:   1.00,
		SalesWeight:      0.75,
		WasteWeight:      0,
		DonateWeight:     0,
		AggregateVersion: 8,
		AggregateID:      1,
	}

	insertResult, err := mgTable.InsertOne(mockInventory)
	if err != nil {
		return nil, err
	}

	log.Println(insertResult)
	return mgTable, nil

}

// type aggQuery struct {
// 	AggregateVersion int64
// 	AggregateID      int64
// }

// func GetMaxAggregateVersion(mgTable *mongo.Collection, aggregateID int64) (int64, error) {
// 	var aggregateVersion int64

// 	versionDocument, err := mgTable.Find(&model.Inventory{
// 		AggregateVersion: int64(10),
// 		AggregateID:      aggregateID,
// 	})
// 	if err != nil {
// 		err = errors.Wrap(err, "Error in retrieve version column")
// 		log.Println(err)
// 		// return nil, err
// 	}

// 	// fmt.Println(versionDocument)

// 	for _, r := range versionDocument {
// 		inventoryItems := r.(*model.Inventory)
// 		aggregateVersion = inventoryItems.AggregateVersion
// 	}

// 	//
// 	// aggregateVersion = 10

// 	return aggregateVersion, nil
// 	// return versionDocument, nil
// }

func StartMongo() *mongo.Collection {
	mgTable, err := InitMongo()
	if err != nil {
		err = errors.Wrap(err, "Unable to get Mongo collection")
		log.Println(ErrorStackTrace(err))
	}

	mgTable, err = InsertMockMongo(mgTable)
	if err != nil {
		err = errors.Wrap(err, "Unable to insert in mongo")
		log.Println(ErrorStackTrace(err))
	}
	return mgTable
}
