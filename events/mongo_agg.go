package events

import (
	"fmt"
	"log"

	mgo "github.com/mongodb/mongo-go-driver/mongo"

	mongo "github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/bhupeshbhatia/go-agg-inventory-cmd/model"

	"github.com/pkg/errors"
)

type aggQuery struct {
	AggregateVersion int64
	AggregateID      int64
}

func GetMaxAggregateVersion(mgTable *mongo.Collection, aggregateID int64) (int64, error) {
	var aggregateVersion int64

	versionDocument, err := mgTable.Find(&model.Inventory{
		AggregateVersion: int64(8),
		AggregateID:      aggregateID,
	})
	if err != nil {
		err = errors.Wrap(err, "Error in retrieve version column")
		log.Println(err)
		// return nil, err
	}

	// fmt.Println(versionDocument)

	for _, r := range versionDocument {
		inventoryItems := r.(*model.Inventory)
		aggregateVersion = inventoryItems.AggregateVersion
	}

	// fmt.Println(aggregateVersion)

	return aggregateVersion, nil
	// return versionDocument, nil
}

func InsertAgg(inventory model.Inventory) (*mgo.InsertOneResult, error) {

	mgTable, err := InitMongo()
	if err != nil {
		err = errors.Wrap(err, "Unable to connect to mongo")
		log.Println(err)
		return nil, err
	}

	insertResult, err := mgTable.InsertOne(inventory)
	if err != nil {
		err = errors.Wrap(err, "Unable to insert event")
		log.Println(err)
		return nil, err
	}

	fmt.Println(insertResult)
	return insertResult, nil

}

func UpdateAgg(inventory model.Inventory) (*mgo.UpdateResult, error) {
	mgTable, err := InitMongo()
	if err != nil {
		err = errors.Wrap(err, "Unable to connect to mongo")
		log.Println(err)
		return nil, err
	}

	updateResult, err := mgTable.UpdateMany(
		&model.Inventory{
			FruitID: inventory.FruitID,
		}, &map[string]interface{}{
			"$set": map[string]interface{}{
				// inventory,
			},
		})
	if err != nil {
		err = errors.Wrap(err, "Unable to update event")
		log.Println(err)
		return nil, err
	}
	fmt.Println(updateResult)

	return updateResult, nil
}

func DeleteAgg(inventory model.Inventory) (*mgo.DeleteResult, error) {
	mgTable, err := InitMongo()
	if err != nil {
		err = errors.Wrap(err, "Unable to connect to mongo")
		log.Println(err)
		return nil, err
	}

	deleteResult, err := mgTable.DeleteMany(
		&model.Inventory{
			FruitID: inventory.FruitID,
		})
	if err != nil {
		err = errors.Wrap(err, "Unable to delete event")
		log.Println(err)
		return nil, err
	}

	fmt.Println(deleteResult)

	return deleteResult, nil
}
