package main

import (
	"fmt"
	"log"

	cs "github.com/TerrexTech/go-cassandrautils/cassandra"
	"github.com/bhupeshbhatia/go-agg-inventory-cmd/events"
	"github.com/bhupeshbhatia/go-agg-inventory-cmd/service"
	"github.com/joho/godotenv"
	"github.com/pkg/errors"
)

func ErrorStackTrace(err error) string {
	return fmt.Sprintf("%+v\n", err)
}

func main() {
	err := godotenv.Load()
	if err != nil {
		err = errors.Wrap(err,
			".env file not found, env-vars will be read as set in environment",
		)
		log.Println(err)
	}

	//For Event store persistent
	// err := callBigCass() //NEED TO COMMENT EVERYTHING TO LOAD PERSISTANT DB.
	// // otherwise you will get 4 values in cassandra.
	// if err != nil {
	// 	err = errors.Wrap(err, "Unable to connect to big Cass")
	// }

	// //FOR MIDDLE CASS
	// err = callMiddleCass()
	// if err != nil {
	// 	err = errors.Wrap(err, "Unable to connect to middle Cass")
	// }

	// preMetaVersion := 13

	// metaVersion, err := events.GetVersion(int64(preMetaVersion))
	// if err != nil {
	// 	err = errors.Wrap(err, "Unable to find event version")
	// 	log.Println(ErrorStackTrace(err))
	// }
	// fmt.Println(metaVersion)

	//MONGO
	aggVersion := callMongo()
	fmt.Println("aggregate_version: ", aggVersion)

	//CHECK IF VERSIONS ARE DIFFERENT
	// 	aggOutOfSync := events.IsAggOutOfSync(metaVersion, aggVersion)
	// 	if aggOutOfSync {
	// 		// events.GetAllPastEvents(aggVersion, metaVersion, tableForGettingEvents())

	// 		//FOR NOW USING MOCK DATA
	// 		//calling mongo and passing mock data
	// 		mockEvent, err := events.MockPastEventsData()
	// 		if err != nil {
	// 			err = errors.Wrap(err,
	// 				"Unable to find event version",
	// 			)
	// 			log.Println(ErrorStackTrace(err))
	// 		}

	// 		for i := range mockEvent {
	// 			events.AggOperations(mockEvent[i])
	// 		}

	// 		//Call InsertAgg in Mongo

	// 	} else {
	// 		fmt.Println("Unable to get past events")
	// 	}

	//Calling inventory service
	insertResult, err := service.AddFoodItem([]byte(service.FoodProduct))
	if err != nil {
		err = errors.Wrap(err, "Unable to find event version")
		log.Println(ErrorStackTrace(err))
	}
	fmt.Println(insertResult)

}

func callMongo() int64 {
	// AGGREGATE_ID
	var aggregateID int64 = 1
	mgTable, err := events.InitMongo()
	if err != nil {
		err = errors.Wrap(err, "Unable to get Mongo collection")
		log.Println(ErrorStackTrace(err))
	}

	// mgTable, err = events.InsertMockMongo(mgTable)
	// if err != nil {
	// 	err = errors.Wrap(err, "Unable to insert in mongo")
	// 	log.Println(ErrorStackTrace(err))
	// }

	aggVersion, err := events.GetMaxAggregateVersion(mgTable, aggregateID)
	if err != nil {
		err = errors.Wrap(err, "Mongo version not received")
		log.Println(ErrorStackTrace(err))
	}
	return aggVersion
}

func callBigCass() error {
	tableDef := events.PersistentStoreDefinition()
	csTable, err := events.InitCassandra(tableDef, "rns_eventstore")
	if err != nil {
		err = errors.Wrap(err, "Cassandra table not initialized")
		log.Println(ErrorStackTrace(err))
		return err
	}

	csTable, err = events.InsertMockPersist(*csTable)
	if err != nil {
		err = errors.Wrap(err, "Cassandra table not initialized")
		log.Println(ErrorStackTrace(err))
		return err
	}
	return nil
}

func callMiddleCass() error {
	tableDef := events.EventMetaDefinition()

	csTable, err := events.InitCassandra(tableDef, "rns_meta")
	if err != nil {
		err = errors.Wrap(err, "Cassandra table not initialized")
		log.Println(ErrorStackTrace(err))
		return err
	}

	csTable, err = events.InsertMockMeta(*csTable)
	if err != nil {
		err = errors.Wrap(err, "Cassandra table not initialized")
		log.Println(ErrorStackTrace(err))
		return err
	}
	return nil
}

func tableForGettingEvents() *cs.Table {
	tableDef := events.PersistentStoreDefinition()
	csTable, err := events.InitCassandra(tableDef, "rns_eventstore")
	if err != nil {
		err = errors.Wrap(err, "Cassandra table not initialized")
		log.Println(ErrorStackTrace(err))
	}
	return csTable
}

//===================================================//
//Mongo

// mgTable, err := mongoaggregate.InitMongo()
// if err != nil {
// 	err = errors.Wrap(err, "Unable to get Mongo collection")
// 	log.Println(ErrorStackTrace(err))
// }

// mgTable, err = mongoaggregate.InsertMockMongo(mgTable)
// if err != nil {
// 	err = errors.Wrap(err, "Unable to insert in mongo")
// 	log.Println(ErrorStackTrace(err))
// }

// //Will pass ID for
// aggregateID := 1

// aggQuery, err := mongoaggregate.GetMaxAggregateVersion(mgTable, int64(aggregateID))
// if err != nil {
// 	err = errors.Wrap(err, "Mongo version not received")
// 	log.Println(ErrorStackTrace(err))
// }

// // var aggVersion []int64

// // for i, r := range aggQuery {
// // 	inventoryItems := r.(*model.Inventory)
// // 	fmt.Println("agg", inventoryItems.AggregateVersion)
// // 	fmt.Println("aggID: ", inventoryItems.AggregateID)
// // 	aggVersion[0] = inventoryItems.AggregateVersion
// // }

// handler.GetAllPastEvents(aggQuery, csTable)

// fmt.Println(aggQuery)
