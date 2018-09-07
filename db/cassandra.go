package db

import (
	"strconv"
	"strings"

	cs "github.com/TerrexTech/go-cassandrautils/cassandra"
	cql "github.com/gocql/gocql"
	"github.com/pkg/errors"
)

type CassandraConfig struct {
	Cluster     *cql.ClusterConfig
	DataCenters *[]string
	Keyspace    string
	Table       string
	TableDef    *map[string]cs.TableColumn
}

func CassandraTable(config *CassandraConfig) (*cs.Table, error) {
	session, err := cs.GetSession(config.Cluster)
	if err != nil {
		err = errors.Wrap(err, "Unable to get Cassandra session")
		return nil, err
	}

	datacenterMap := map[string]int{}
	for _, dataCenterString := range *config.DataCenters {
		dataCenter := strings.Split(dataCenterString, ":")
		centerID, err := strconv.Atoi(dataCenter[1])
		if err != nil {
			return nil, errors.Wrap(err, "Cassandra Keyspace create error - mismatch format")
		}
		datacenterMap[dataCenter[0]] = centerID
	}

	keyspaceConfig := cs.KeyspaceConfig{
		Name:                    config.Keyspace,
		ReplicationStrategy:     "NetworkTopologyStrategy",
		ReplicationStrategyArgs: datacenterMap,
	}
	keyspace, err := cs.NewKeyspace(session, keyspaceConfig)
	if err != nil {
		err = errors.Wrap(err, "Unable to create Cassandra Keyspace")
		return nil, err
	}

	tableConfig := cs.TableConfig{
		Keyspace: keyspace,
		Name:     config.Table,
	}

	table, err := cs.NewTable(session, &tableConfig, config.TableDef)
	if err != nil {
		err = errors.Wrap(err, "Unable to create Cassandra table")
		return nil, err
	}
	return table, nil
}
