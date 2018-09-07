package events

import (
	"log"

	"github.com/TerrexTech/go-mongoutils/mongo"
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
