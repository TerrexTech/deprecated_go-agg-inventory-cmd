package model

type EventStoreMeta struct {
	// AggregateVersion tracks the version to be used
	// by new events for that aggregate.
	AggregateVersion int64 `json:"aggregate_version"`
	// AggregateID corresponds to AggregateID in
	// event-store and ID in aggregate-projection.
	AggregateID int8 `json:"aggregate_id"`
	// Year bucket is the year in which the event was generated.
	// This is used as the partitioning key.
	YearBucket int16 `json:"year_bucket"`
}
