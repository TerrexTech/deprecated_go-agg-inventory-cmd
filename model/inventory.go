package model

import (
	"time"

	"github.com/bhupeshbhatia/go-agg-inventory-cmd/test2"
)

//Inventory represents inventory collection
type Inventory struct {
	FruitID          int64          `bson:"fruit_id,omitempty" json:"fruit_id,omitempty"`
	RsCustomerID     string         `bson:"rs_customer_id,omitempty" json:"rs_customer_id,omitempty"`
	Name             string         `bson:"name,omitempty" json:"name,omitempty"`
	Origin           string         `bson:"origin,omitempty" json:"origin,omitempty"`
	DateArrived      time2.JsonTime `bson:"date_arrived,omitempty" json:"date_arrived,omitempty"`
	DateSold         time.Time      `bson:"date_sold,omitempty" json:"date_sold,omitempty"`
	DeviceID         int64          `bson:"device_id,omitempty" json:"device_id,omitempty"`
	SalePrice        float64        `bson:"sale_price,omitempty" json:"sale_price,omitempty"`
	OriginalWeight   float64        `bson:"original_weight,omitempty" json:"original_weight,omitempty"`
	SalesWeight      float64        `bson:"sales_weight,omitempty" json:"sales_weight,omitempty"`
	WasteWeight      float64        `bson:"waste_weight,omitempty" json:"waste_weight,omitempty"`
	DonateWeight     float64        `bson:"donate_weight,omitempty" json:"donate_weight,omitempty"`
	AggregateVersion int64          `bson:"aggregate_version,omitempty" json:"aggregate_version,omitempty"`
	AggregateID      int64          `bson:"aggregate_id,omitempty" json:"aggregate_id,omitempty"`
}
