package model

import "time"

//Inventory represents inventory collection
type Inventory struct {
	FruitID          int       `bson:"fruit_id,omitempty" json:"fruit_id,omitempty"`
	RsCustomerID     string    `bson:"rs_customer_id,omitempty" json:"rs_customer_id,omitempty"`
	Name             string    `bson:"name,omitempty" json:"name,omitempty"`
	DateBought       time.Time `bson:"date_bought,omitempty" json:"date_bought,omitempty"`
	DateSold         time.Time `bson:"date_sold,omitempty" json:"date_sold,omitempty"`
	SalePrice        float64   `bson:"sale_price,omitempty" json:"sale_price,omitempty"`
	OriginalWeight   float64   `bson:"original_weight,omitempty" json:"original_weight,omitempty"`
	SalesWeight      float64   `bson:"sales_weight,omitempty" json:"sales_weight,omitempty"`
	WasteWeight      float64   `bson:"waste_weight,omitempty" json:"waste_weight,omitempty"`
	DonateWeight     float64   `bson:"donate_weight,omitempty" json:"donate_weight,omitempty"`
	AggregateVersion int64     `bson:"aggregate_version,omitempty" json:"aggregate_version,omitempty"`
	AggregateID      int64     `bson:"aggregate_id,omitempty" json:"aggregate_id,omitempty"`
}
