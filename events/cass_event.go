package events

import (
	"log"
	"time"

	cs "github.com/TerrexTech/go-cassandrautils/cassandra"
	"github.com/bhupeshbhatia/go-agg-inventory-cmd/model"
	"github.com/gocql/gocql"
	"github.com/pkg/errors"
)

type dataStruct struct {
	Action           string
	Data             string
	Timestamp        time.Time
	UserID           int
	UUID             gocql.UUID
	YearBucket       uint16
	AggregateVersion int64
	AggregateID      int64
}

func GetVersion(csTable *cs.Table, metaVersion int64) (int64, error) {
	// csTable, err := initCassandra()
	// if err != nil {
	// 	err = errors.Wrap(err, "Cassandra table not initialized")
	// 	return 0, err
	// }

	eventInfo := []model.EventStoreMeta{
		model.EventStoreMeta{
			AggregateVersion: metaVersion,
		},
	}

	//Update Version
	newVersion := eventInfo[0].AggregateVersion + 1

	//Adding newVersion to eventInfo
	eventInfo[0].AggregateVersion = newVersion

	// err := <-csTable.AsyncInsert(eventInfo[0])
	// if err != nil {
	// 	err = errors.Wrap(err, "Error updating event info for Aggregate")
	// 	return 0, err
	// }
	// return eventInfo[0].AggregateVersion, nil
	return newVersion - 1, nil
}

func IsAggOutOfSync(eventVersion int64, aggVersion int64) bool {
	if eventVersion > aggVersion {
		return true
	}
	//WAIT FOR EVENT FROM KAFKA
	return false
}

func GetAllPastEvents(aggVersion int64, metaVersion int64, t *cs.Table) {
	versionCol, err := t.Column("version")
	yearCol, err := t.Column("yearBucket")
	aggIDCol, err := t.Column("aggregateID")

	if err != nil {
		err = errors.Wrap(err, "No column received")
		log.Println(ErrorStackTrace(err))
		return
	}

	colValues := []cs.ColumnComparator{
		cs.ColumnComparator{
			Name:  yearCol,
			Value: 2018,
		}.Eq(),
		cs.ColumnComparator{
			Name:  aggIDCol,
			Value: 1,
		}.Eq(),
		cs.ColumnComparator{
			Name:  versionCol,
			Value: metaVersion,
		}.Lt(),
		cs.ColumnComparator{
			Name:  versionCol,
			Value: aggVersion,
		}.Gt(),
	}

	bind := []dataStruct{}
	sp := cs.SelectParams{
		ColumnValues:  colValues,
		PageSize:      10,
		SelectColumns: t.Columns(), //will this select all columns
		ResultsBind:   &bind,
	}
	fetched, err := t.Select(sp)

	if err != nil {
		log.Fatalln(err)
	} else {
		log.Println("Printing Fetched Data:")
		log.Println(fetched)
	}
}
