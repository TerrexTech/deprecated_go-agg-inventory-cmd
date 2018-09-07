package events

import (
	"fmt"
	"os"
	"time"

	cs "github.com/TerrexTech/go-cassandrautils/cassandra"
	"github.com/TerrexTech/go-commonutils/utils"
	"github.com/bhupeshbhatia/go-agg-inventory-cmd/db"
	"github.com/bhupeshbhatia/go-agg-inventory-cmd/model"
	cql "github.com/gocql/gocql"
	"github.com/pkg/errors"
)

func ErrorStackTrace(err error) string {
	return fmt.Sprintf("%+v\n", err)
}

// AggregateID is the aggregate-id (as stored in event-store)
// for the auth-user aggregate.
const AggregateID = 0
const AggregateVersion = 0

// CASSANDRA_KEYSPACE=rns_eventstore

// type EventStoreMeta struct {
// 	// AggregateVersion tracks the version to be used
// 	// by new events for that aggregate.
// 	AggregateVersion int64 `json:"aggregate_version"`
// 	// AggregateID corresponds to AggregateID in
// 	// event-store and ID in aggregate-projection.
// 	AggregateID int8 `json:"aggregate_id"`
// 	// Year bucket is the year in which the event was generated.
// 	// This is used as the partitioning key.
// 	YearBucket int16 `json:"year_bucket"`
// }

type PersistStore struct {
	Action      string
	Data        string
	Timestamp   time.Time
	UserID      int
	UUID        cql.UUID
	YearBucket  uint16
	Version     int64
	AggregateID int64
}

func InitCassandra(tableDef *map[string]cs.TableColumn, keyspace string) (*cs.Table, error) {
	hosts := os.Getenv("CASSANDRA_HOSTS")
	dataCenters := os.Getenv("CASSANDRA_DATA_CENTERS")
	username := os.Getenv("CASSANDRA_USERNAME")
	password := os.Getenv("CASSANDRA_PASSWORD")
	// keyspaceName := os.Getenv("CASSANDRA_KEYSPACE")
	keyspaceName := keyspace
	tableName := os.Getenv("CASSANDRA_TABLE")

	// tableDef = tableDefinition()

	clusterHosts := *utils.ParseHosts(hosts)
	cluster := cql.NewCluster(clusterHosts...)
	cluster.ConnectTimeout = time.Millisecond * 3000
	cluster.Timeout = time.Millisecond * 3000
	cluster.ProtoVersion = 4

	if username != "" && password != "" {
		cluster.Authenticator = cql.PasswordAuthenticator{
			Username: username,
			Password: password,
		}
	}

	config := &db.CassandraConfig{
		Cluster:     cluster,
		DataCenters: utils.ParseHosts(dataCenters),
		Keyspace:    keyspaceName,
		Table:       tableName,
		TableDef:    tableDef,
	}
	return db.CassandraTable(config)
}

func EventMetaDefinition() *map[string]cs.TableColumn {
	tableDef := &map[string]cs.TableColumn{
		"aggregateVersion": cs.TableColumn{
			Name:            "aggregate_version",
			DataType:        "int",
			PrimaryKeyIndex: "2",
			PrimaryKeyOrder: "DESC",
		},
		"aggregateId": cs.TableColumn{
			Name:            "aggregate_id",
			DataType:        "int",
			PrimaryKeyIndex: "1",
		},
		"yearBucket": cs.TableColumn{
			Name:            "year_bucket",
			DataType:        "smallint",
			PrimaryKeyIndex: "0",
		},
	}
	return tableDef
}

//NEED TO REMOVE THIS
func InsertMockMeta(csTable cs.Table) (*cs.Table, error) {

	mockEvent := model.EventStoreMeta{
		AggregateVersion: 13,
		AggregateID:      1,
		YearBucket:       2018,
	}

	err := <-csTable.AsyncInsert(mockEvent)
	if err != nil {
		return nil, err
	}
	return &csTable, nil
}

func PersistentStoreDefinition() *map[string]cs.TableColumn {
	tableDef := &map[string]cs.TableColumn{
		"action": cs.TableColumn{
			Name:            "action",
			DataType:        "text",
			PrimaryKeyIndex: "3",
		},
		"aggregateID": cs.TableColumn{
			Name:            "aggregate_id",
			DataType:        "int",
			PrimaryKeyIndex: "1",
		},
		"data": cs.TableColumn{
			Name:     "data",
			DataType: "text",
		},
		"timestamp": cs.TableColumn{
			Name:            "timestamp",
			DataType:        "timestamp",
			PrimaryKeyIndex: "4",
			PrimaryKeyOrder: "DESC",
		},
		"userID": cs.TableColumn{
			Name:     "user_id",
			DataType: "int",
		},
		"uuid": cs.TableColumn{
			Name:            "uuid",
			DataType:        "uuid",
			PrimaryKeyIndex: "5",
		},
		"version": cs.TableColumn{
			Name:            "version",
			DataType:        "int",
			PrimaryKeyIndex: "2",
			PrimaryKeyOrder: "DESC",
		},
		"yearBucket": cs.TableColumn{
			Name:            "year_bucket",
			DataType:        "smallint",
			PrimaryKeyIndex: "0",
		},
	}
	return tableDef
}

func MockDataPersist() []PersistStore {

	genUUID, err := cql.RandomUUID()
	if err != nil {
		err = errors.Wrapf(err, "Error generating UUID")
		return nil
	}
	mockEvent := []PersistStore{
		PersistStore{
			Action:      "insert",
			Data:        "Test",
			Timestamp:   time.Now(),
			UserID:      1,
			UUID:        genUUID,
			YearBucket:  2018,
			Version:     11,
			AggregateID: 1,
		},
		PersistStore{
			Action:      "update",
			Data:        "Second Test",
			Timestamp:   time.Now().Add(20),
			UserID:      2,
			UUID:        genUUID,
			YearBucket:  2018,
			Version:     12,
			AggregateID: 1,
		},
	}

	return mockEvent
}

//NEED TO REMOVE THIS
func InsertMockPersist(csTable cs.Table) (*cs.Table, error) {

	mockEvent := MockDataPersist()

	for _, v := range mockEvent {
		err := <-csTable.AsyncInsert(v)
		if err != nil {
			return nil, err
		}
	}

	return &csTable, nil
}

// func GetVersion(csTable *cs.Table) (int64, error) {
// 	// csTable, err := initCassandra()
// 	// if err != nil {
// 	// 	err = errors.Wrap(err, "Cassandra table not initialized")
// 	// 	return 0, err
// 	// }

// 	eventInfo := []EventStoreMeta{
// 		EventStoreMeta{
// 			AggregateVersion: 9,
// 		},
// 	}

// 	//Update Version
// 	newVersion := eventInfo[0].AggregateVersion + 1

// 	//Adding newVersion to eventInfo
// 	eventInfo[0].AggregateVersion = newVersion

// 	err := <-csTable.AsyncInsert(eventInfo[0])
// 	if err != nil {
// 		err = errors.Wrapf(err, "Error updating event info for Aggregate: %d", AggregateID)
// 		return 0, err
// 	}
// 	// return eventInfo[0].AggregateVersion, nil
// 	return newVersion, nil
// }

//MOCK DATA FOR PAST EVENTS
