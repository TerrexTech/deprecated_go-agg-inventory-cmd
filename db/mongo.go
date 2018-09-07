package db

import (
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/bhupeshbhatia/go-agg-inventory-cmd/model"
	mgo "github.com/mongodb/mongo-go-driver/mongo"
	"github.com/pkg/errors"
)

type MongoConfig struct {
	Client     *mgo.Client
	TimeoutMS  int16
	Database   string
	Collection string
}

func MongoCollection(config *MongoConfig) (*mongo.Collection, error) {
	//Found in go-mongo Utils
	conn := &mongo.ConnectionConfig{
		Client:  config.Client,
		Timeout: 5000,
	}

	//Index configuration
	// indexConfigs := []mongo.IndexConfig{
	// 	mongo.IndexConfig{
	// 		ColumnConfig: []mongo.IndexColumnConfig{
	// 			mongo.IndexColumnConfig{
	// 				Name:        "fruit_id",
	// 				IsDescOrder: true,
	// 			},
	// 			mongo.IndexColumnConfig{
	// 				Name:        "rs_customer_id",
	// 				IsDescOrder: true,
	// 			},
	// 			mongo.IndexColumnConfig{
	// 				Name:        "name",
	// 				IsDescOrder: true,
	// 			},
	// 			mongo.IndexColumnConfig{
	// 				Name:        "date_bought",
	// 				IsDescOrder: true,
	// 			},
	// 			mongo.IndexColumnConfig{
	// 				Name:        "date_sold",
	// 				IsDescOrder: true,
	// 			},
	// 			mongo.IndexColumnConfig{
	// 				Name:        "sale_price",
	// 				IsDescOrder: true,
	// 			},
	// 			mongo.IndexColumnConfig{
	// 				Name:        "original_weight",
	// 				IsDescOrder: true,
	// 			},
	// 			mongo.IndexColumnConfig{
	// 				Name:        "sales_weight",
	// 				IsDescOrder: true,
	// 			},
	// 			mongo.IndexColumnConfig{
	// 				Name:        "waste_weight",
	// 				IsDescOrder: true,
	// 			},
	// 			mongo.IndexColumnConfig{
	// 				Name:        "donate_weight",
	// 				IsDescOrder: true,
	// 			},
	// 		},
	// 		IsUnique: true,
	// 		Name:     "inventory_index",
	// 	},
	// }

	c := &mongo.Collection{
		Connection:   conn,
		Name:         config.Collection,
		Database:     config.Database,
		SchemaStruct: &model.Inventory{},
		// Indexes:      indexConfigs,
	}

	coll, err := mongo.EnsureCollection(c)
	if err != nil {
		err = errors.Wrap(err, "Unable to create Mongo collection")
		return nil, err
	}
	return coll, nil
}
