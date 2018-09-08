package events

import (
	"fmt"
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

func GetVersion(metaVersion int64) (int64, error) {
	eventInfo := []model.EventStoreMeta{
		model.EventStoreMeta{
			AggregateVersion: metaVersion,
		},
	}
	//Update Version
	eventInfo[0].AggregateVersion++

	return eventInfo[0].AggregateVersion - 1, nil
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

//mock event in - need to change it
func AggOperations(event MockEvent) {
	switch event.Action {
	case "insert":
		aggOp, err := InsertAgg(event.Data)
		if err != nil {
			err = errors.Wrap(err, "Unable to Insert to mongo")
			log.Println(err)
		}
		fmt.Println("Insert:", aggOp)
	case "update":
		aggOp, err := UpdateAgg(event.Data)
		if err != nil {
			err = errors.Wrap(err, "Unable to Update mongo")
			log.Println(err)
		}
		fmt.Println("Update: ", aggOp)
	case "delete":
		aggOp, err := DeleteAgg(event.Data)
		if err != nil {
			err = errors.Wrap(err, "Unable to Update mongo")
			log.Println(err)
		}
		fmt.Println("Delete", aggOp)
	}
}
