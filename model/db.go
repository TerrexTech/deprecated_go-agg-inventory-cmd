package model

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"time"

	mongo "github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/mongodb/mongo-go-driver/bson"
	mgo "github.com/mongodb/mongo-go-driver/mongo"
	"github.com/pkg/errors"
)

type Datastore interface {
	Collection() *mongo.Collection
	CreateDataMongo(numOfVal int) ([]byte, error)
	SearchByDate(body []byte) ([]byte, error)
	SearchByKeyVal(body []byte) ([]byte, error)
	AddInventory(body []byte) ([]byte, error)
	UpdateInventory(body []byte) (*mgo.UpdateResult, error)
	DeleteInventory(body []byte) ([]byte, error)
	CompareInvGraph(toSearch []byte, invResultsAfterSearch []byte) ([]byte, error)
	ProdSoldPerHour(toSearch []byte, invResultsAfterSearch []byte) ([]byte, error)
	DistByWeight() ([]byte, error)
	GenForAddInv() ([]byte, error)
}

type Db struct {
	collection *mongo.Collection
}

type DbConfig struct {
	Hosts               []string
	Username            string
	Password            string
	TimeoutMilliseconds uint32
	Database            string
	Collection          string
}

//===============================================
type InvSearchDate struct {
	EndDate   int64 `bson:"end_date,omitempty" json:"end_date,omitempty"`
	StartDate int64 `bson:"start_date,omitempty" json:"start_date,omitempty"`
}

type InvSearchKeyVal struct {
	SearchKey string      `bson:"search_key,omitempty" json:"search_key,omitempty"`
	SearchVal interface{} `bson:"search_val,omitempty" json:"search_val,omitempty"`
}

func (i *InvSearchKeyVal) UnmarshalJSON(search []byte) error {
	m := make(map[string]interface{})
	err := json.Unmarshal(search, &m)
	if err != nil {
		err = errors.Wrap(err, "Unmarshal Error")
		return err
	}

	if m["search_key"] != nil {
		i.SearchKey = m["search_key"].(string)
	}

	test := reflect.TypeOf(m["search_val"]).Kind()
	log.Println(test)

	if i.SearchKey == "name" || i.SearchKey == "origin" || i.SearchKey == "location" {
		searchType := reflect.TypeOf(m["search_val"]).Kind()
		if m["search_val"] != nil && searchType == reflect.String {
			i.SearchVal = m["search_val"].(string)
		}
	}

	if i.SearchKey == "upc" || i.SearchKey == "sku" || i.SearchKey == "arrival_date" || i.SearchKey == "expiry_date" || i.SearchKey == "prod_quantity" {
		searchType := reflect.TypeOf(m["search_val"]).Kind()
		if m["search_val"] != nil && searchType != reflect.Int64 {
			val, err := strconv.Atoi((m["search_val"]).(string))
			if err != nil {
				err = errors.Wrap(err, "Cannot convert search_val to Int64s - UnmarshalJSON - InvSearchKeyVal")
				return err
			}
			i.SearchVal = int64(val)
		}

	}

	if i.SearchKey == "sale_price" || i.SearchKey == "sold_weight" {
		searchType := reflect.TypeOf(m["search_val"]).Kind()
		if m["search_val"] != nil && searchType != reflect.Float64 {
			val, err := strconv.ParseFloat((m["search_val"]).(string), 64)
			if err != nil {
				err = errors.Wrap(err, "Cannot convert search_val to Float64 - UnmarshalJSON - InvSearchKeyVal")
				return err
			}
			i.SearchVal = val
		}
	}
	return nil
}

type InvDashboard struct {
	ProdName     string  `bson:"prod_name,omitempty" json:"prod_name,omitempty"`
	ProdWeight   float64 `bson:"prod_weight,omitempty" json:"prod_weight,omitempty"`
	TotalWeight  float64 `bson:"total_weight,omitempty" json:"total_weight,omitempty"`
	SoldWeight   float64 `bson:"sold_weight,omitempty" json:"sold_weight,omitempty"`
	WasteWeight  float64 `bson:"waste_weight,omitempty" json:"waste_weight,omitempty"`
	ProductSold  int64   `bson:"prod_sold,omitempty" json:"prod_sold,omitempty"`
	DonateWeight float64 `bson:"donate_weight,omitempty" json:"donate_weight,omitempty"`
	Dates        int64   `bson:"dates,omitempty" json:"dates,omitempty"`
}

func ConfirmDbExists(dbConfig DbConfig) (*Db, error) {
	clientConfig := mongo.ClientConfig{
		Hosts:               dbConfig.Hosts,
		Username:            dbConfig.Username,
		Password:            dbConfig.Password,
		TimeoutMilliseconds: 3000,
	}

	// ====> MongoDB Client
	client, err := mongo.NewClient(clientConfig)
	if err != nil {
		err = errors.Wrap(err, "Mongo client not available")
		return nil, err
	}

	conn := &mongo.ConnectionConfig{
		Client:  client,
		Timeout: 5000,
	}

	// Index Configuration
	indexConfigs := []mongo.IndexConfig{
		mongo.IndexConfig{
			ColumnConfig: []mongo.IndexColumnConfig{
				mongo.IndexColumnConfig{
					Name:        "item_id", ////CAN'T HAVE THIS AS UNIQUE
					IsDescOrder: false,
				},
			},
			IsUnique: true,
			Name:     "item_id_index",
		},
	}

	// ====> Create New Collection
	colConfig := &mongo.Collection{
		Connection:   conn,
		Name:         dbConfig.Collection,
		Database:     dbConfig.Database,
		SchemaStruct: &Inventory{},
		Indexes:      indexConfigs,
	}
	c, err := mongo.EnsureCollection(colConfig)
	if err != nil {
		err = errors.Wrap(err, "Error creating DB-client")
		return nil, err
	}

	return &Db{
		collection: c,
	}, nil

}

// Collection returns the currrent MongoDB collection
func (d *Db) Collection() *mongo.Collection {
	return d.collection
}

func (db *Db) CreateDataMongo(numOfVal int) ([]byte, error) {
	newInventory := []Inventory{}
	for i := 0; i < numOfVal; i++ {
		newInventory = append(newInventory, GenerateDataForInv())
	}

	for _, v := range newInventory {
		insertResult, err := db.collection.InsertOne(v)
		if err != nil {
			err = errors.Wrap(err, "Unable to insert data")
			log.Println(err)
			return nil, err
		}
		log.Println(insertResult)
	}

	marInv, err := json.Marshal(&newInventory)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	return marInv, nil
}

func (db *Db) SearchByDate(body []byte) ([]byte, error) {
	searchInDb := []InvSearchDate{}

	var findResults []interface{}

	//Read the body - so unmarshal
	err := json.Unmarshal(body, &searchInDb)
	if err != nil {
		err = errors.Wrap(err, "Unable to unmarshal - SearchDb")
		log.Println(err)
		return nil, err
	}

	for _, val := range searchInDb {
		if val.StartDate != 0 && val.EndDate != 0 {
			//Find
			findResults, err = db.collection.Find(map[string]interface{}{
				"timestamp": map[string]int64{
					"$lte": val.EndDate,
					"$gte": val.StartDate,
				},
			})
		}

		if val.StartDate == 0 && val.EndDate != 0 {
			findResults, err = db.collection.Find(map[string]interface{}{
				"timestamp": map[string]int64{
					"$lte": val.EndDate,
				},
			})
		}
	}

	if err != nil {
		err = errors.Wrap(err, "Error while fetching product.")
		log.Println(err)
		return nil, err
	}

	//length
	if len(findResults) == 0 {
		msg := "No results found - SearchInDb"
		return nil, errors.New(msg)
	}

	inventory := []Inventory{}

	for _, v := range findResults {
		resultInv := v.(*Inventory)
		inventory = append(inventory, *resultInv)
	}

	//Marshal into []byte
	totalResult, err := json.Marshal(&inventory)
	if err != nil {
		err = errors.Wrap(err, "Unable to create response body")
		log.Println(err)
		return nil, err
	}
	return totalResult, nil
}

func (db *Db) SearchByKeyVal(body []byte) ([]byte, error) {
	searchInDb := []InvSearchKeyVal{}

	var findResults []interface{}

	//Read the body - so unmarshal
	err := json.Unmarshal(body, &searchInDb)
	if err != nil {
		err = errors.Wrap(err, "Unable to unmarshal - SearchDb")
		log.Println(err)
		return nil, err
	}

	for _, v := range searchInDb {
		if v.SearchKey != "" && v.SearchVal != "" {
			findResults, err = db.collection.Find(map[string]interface{}{
				v.SearchKey: map[string]interface{}{
					"$eq": &v.SearchVal,
				},
			})
		}
	}

	if err != nil {
		err = errors.Wrap(err, "Error while fetching product.")
		log.Println(err)
		return nil, err
	}

	//length
	if len(findResults) == 0 {
		msg := "No results found - SearchInDb"
		return nil, errors.New(msg)
	}

	inventory := []Inventory{}

	for _, v := range findResults {
		resultInv := v.(*Inventory)
		inventory = append(inventory, *resultInv)
	}

	//Marshal into []byte
	totalResult, err := json.Marshal(&inventory)
	if err != nil {
		err = errors.Wrap(err, "Unable to create response body")
		log.Println(err)
		return nil, err
	}
	return totalResult, nil
}

// func (db *Db) SearchByDate(body []byte) ([]byte, error) {
// 	searchInDb := []InvSearchDate{}

// 	var findResults []interface{}

// 	//Read the body - so unmarshal
// 	err := json.Unmarshal(body, &searchInDb)
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to unmarshal - SearchDb")
// 		log.Println(err)
// 		return nil, err
// 	}

// 	for _, val := range searchInDb {
// 		if val.StartDate != 0 && val.EndDate != 0 {
// 			//Find
// 			findResults, err = db.collection.Find(map[string]interface{}{
// 				"timestamp": map[string]int64{
// 					"$lte": val.EndDate,
// 					"$gte": val.StartDate,
// 				},
// 			})
// 		}

// 		if val.StartDate == 0 && val.EndDate != 0 {
// 			findResults, err = db.collection.Find(map[string]interface{}{
// 				"timestamp": map[string]int64{
// 					"$lte": val.EndDate,
// 				},
// 			})
// 		}
// 	}

// 	if err != nil {
// 		err = errors.Wrap(err, "Error while fetching product.")
// 		log.Println(err)
// 		return nil, err
// 	}

// 	//length
// 	if len(findResults) == 0 {
// 		msg := "No results found - SearchInDb"
// 		return []byte(msg), nil
// 	}

// 	inventory := []Inventory{}

// 	for _, v := range findResults {
// 		resultInv := v.(*Inventory)
// 		inventory = append(inventory, *resultInv)
// 	}

// 	//Marshal into []byte
// 	totalResult, err := json.Marshal(&inventory)
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to create response body")
// 		log.Println(err)
// 		return nil, err
// 	}
// 	return totalResult, nil
// }

func (db *Db) AddInventory(body []byte) ([]byte, error) {

	// var insertResult *mgo.InsertOneResult
	var insCount int

	//Convert body of type []byte into type []model.Inventory{}
	inventory := Inventory{}
	err := json.Unmarshal(body, &inventory)
	if err != nil {
		err = errors.Wrap(err, "Unable to unmarshal AddInventory")
		log.Println(err)
		return nil, err
	}

	itemId := inventory.ItemID.String()

	if inventory.UPC != 0 && inventory.SKU != 0 && itemId != "" { //need to change this
		inventory.Timestamp = time.Now().Unix()

		insertResult, err := db.collection.InsertOne(inventory)
		if err != nil {
			err = errors.Wrap(err, "Unable to insert - AddInventory")
			log.Println(err)
			return nil, err
		}
		log.Println("Addddddddddddd", insertResult)
		insCount = insCount + 1
	}

	insCountStr := strconv.Itoa(insCount)
	log.Println(insCountStr)
	return []byte(insCountStr), nil
}

func (db *Db) UpdateInventory(body []byte) (*mgo.UpdateResult, error) {
	inventory := Inventory{}
	log.Println("^^^^^^^^^^^^^^^^", string(body))
	err := json.Unmarshal(body, &inventory)
	if err != nil {
		err = errors.Wrap(err, "Unable to unmarshal - UpdateInventory")
		log.Println(err)
		return nil, err
	}
	log.Println(inventory.DeviceID)

	//Filter required for Update
	filter := &Inventory{
		SKU: inventory.SKU,
		UPC: inventory.UPC,
	}

	if inventory.UPC == 0 {
		log.Println("UPC is empty")
		return nil, errors.New("UPC not found")
	}

	if inventory.SKU == 0 {
		log.Println("SKU is empty")
		return nil, errors.New("SKU not found")
	}

	if inventory.Name == "" {
		log.Println("Name is empty")
		return nil, errors.New("Name not found")
	}

	if inventory.Origin == "" {
		log.Println("Origin is empty")
		return nil, errors.New("Origin not found")
	}

	if inventory.DateArrived == 0 {
		log.Println("Date arrived is empty")
		return nil, errors.New("Date arrived not found")
	}

	if inventory.DeviceID.String() == "" {
		log.Println("DeviceID is empty")
		return nil, errors.New("DeviceID not found")
	}

	if inventory.Price == 0 {
		log.Println("Price is empty")
		return nil, errors.New("Price not found")
	}

	if inventory.TotalWeight == 0 {
		log.Println("Total weight is empty")
		return nil, errors.New("Total weight not found")
	}

	if inventory.Location == "" {
		log.Println("Location is empty")
		return nil, errors.New("Location not found")
	}

	//Adding the timestamp
	nowTime := time.Now().Unix()
	inventory.Timestamp = nowTime

	update := &map[string]interface{}{
		"upc":          inventory.UPC,
		"sku":          inventory.SKU,
		"name":         inventory.Name,
		"origin":       inventory.Origin,
		"date_arrived": inventory.DateArrived,
		"device_id":    inventory.DeviceID.String(),
		"price":        inventory.Price,
		"total_weight": inventory.TotalWeight,
		"location":     inventory.Location,
	}

	updateResult, err := db.collection.UpdateMany(filter, update)
	if err != nil {
		err = errors.Wrap(err, "Unable to update event")
		return nil, err
	}
	fmt.Println(updateResult)

	return updateResult, nil

}

func (db *Db) DeleteInventory(body []byte) ([]byte, error) {
	var delCount int
	//Convert body of type []byte into type []model.Inventory{}
	inventory := []Inventory{}
	//Convert body of type []byte into type []model.Inventory{}
	err := json.Unmarshal(body, &inventory)
	if err != nil {
		err = errors.Wrap(err, "Unable to unmarshal - DeleteInventory")
		log.Println(err)
		return nil, err
	}

	for _, inVal := range inventory {
		deleteResult, err := db.collection.DeleteMany(&Inventory{
			ItemID: inVal.ItemID,
		})
		if err != nil {
			err = errors.Wrap(err, "Unable to delete event")
			log.Println(err)
			return nil, err
		}
		if deleteResult.DeletedCount > 0 {
			delCount = delCount + 1
		}
		log.Println(deleteResult)
	}
	count := strconv.Itoa(delCount)
	return []byte(count), nil
}

func (db *Db) CompareInvGraph(toSearch []byte, invResultsAfterSearch []byte) ([]byte, error) {
	var totalWeight float64
	var soldWeight float64
	var wasteWeight float64
	var donateWeight float64

	var tweight []float64
	var sweight []float64
	var wweight []float64
	var dweight []float64

	invDashGraph := []InvDashboard{}
	//Convert body of type []byte into type []model.Inventory{}

	log.Println(string(invResultsAfterSearch))
	err := json.Unmarshal(invResultsAfterSearch, &invDashGraph)
	if err != nil {
		err = errors.Wrap(err, "Unable to unmarshal - CompareInvGraph")
		log.Println(err)
		return nil, err
	}

	for _, v := range invDashGraph {
		totalWeight = v.TotalWeight + totalWeight
		tweight = append(tweight, totalWeight)

		soldWeight = v.SoldWeight + soldWeight
		sweight = append(sweight, soldWeight)

		wasteWeight = v.WasteWeight + wasteWeight
		wweight = append(wweight, wasteWeight)

		donateWeight = v.DonateWeight + donateWeight
		dweight = append(dweight, donateWeight)
	}

	invSearch := []InvSearchDate{}

	err = json.Unmarshal(toSearch, &invSearch)
	if err != nil {
		err = errors.Wrap(err, "Unable to Unmarshal timestamp from body - TwSaleWasteDonate")
		log.Println(err)
		return nil, err
	}

	log.Println(invSearch)
	var totalResult []byte

	dash := make(map[int]InvDashboard)
	// dash := []InvDashboard{}
	for i, v := range invSearch {
		dash[i] = InvDashboard{
			TotalWeight:  tweight[i],
			SoldWeight:   sweight[i],
			WasteWeight:  wweight[i],
			DonateWeight: dweight[i],
			Dates:        v.EndDate,
		}
	}

	totalResult, err = json.Marshal(dash)
	if err != nil {
		err = errors.Wrap(err, "Unable to create response body")
		log.Println(err)
		return nil, err
	}

	return totalResult, nil
}

func (db *Db) ProdSoldPerHour(toSearch []byte, invResultsAfterSearch []byte) ([]byte, error) {
	var soldWeight float64
	var sweight []float64

	invProdGraph := []InvDashboard{}
	//Convert body of type []byte into type []model.Inventory{}
	err := json.Unmarshal(invResultsAfterSearch, &invProdGraph)
	if err != nil {
		err = errors.Wrap(err, "Unable to unmarshal - ProdSoldPerHour")
		log.Println(err)
		return nil, err
	}

	for _, v := range invProdGraph {
		soldWeight = v.SoldWeight + soldWeight
		sweight = append(sweight, soldWeight)
	}

	invSearch := []InvSearchDate{}
	err = json.Unmarshal(toSearch, &invSearch)
	if err != nil {
		err = errors.Wrap(err, "Unable to Unmarshal timestamp from body - TwSaleWasteDonate")
		log.Println(err)
		return nil, err
	}

	var totalResult []byte

	dash := make(map[int]InvDashboard)
	for i, v := range invSearch {

		dash[i] = InvDashboard{
			SoldWeight: sweight[i],
			Dates:      v.StartDate,
		}

		log.Println(dash)
	}

	totalResult, err = json.Marshal(dash)
	if err != nil {
		err = errors.Wrap(err, "Unable to create response body")
		log.Println(err)
		return nil, err
	}
	return totalResult, nil
}

func (db *Db) DistByWeight() ([]byte, error) {
	pipeline := bson.NewArray(
		bson.VC.Document(
			bson.NewDocument(
				bson.EC.SubDocumentFromElements(
					"$group",
					bson.EC.String("_id", "$name"),
					bson.EC.SubDocumentFromElements(
						"total",
						bson.EC.String("$sum", "$total_weight"),
					),
				),
			),
		),
	)
	aggResults, err := db.collection.Aggregate(pipeline)
	if err != nil {
		log.Fatalln(err)
	}
	// log.Println(aggResults)

	dist := []InvDashboard{}

	for _, v := range aggResults {
		value := v.(map[string]interface{})
		strValue := value["_id"].(string)
		secValue := value["total"].(float64)
		dist = append(dist, InvDashboard{
			ProdName:   strValue,
			ProdWeight: secValue,
		})
	}

	distWeight, err := json.Marshal(&dist)
	if err != nil {
		err = errors.Wrap(err, "Unable to marshal distribution")
		log.Println(err)
		return nil, err
	}

	return distWeight, nil
}

func (db *Db) GenForAddInv() ([]byte, error) {
	genData := GenerateDataForInv()

	inventory := []Inventory{}
	inventory = append(inventory, genData)

	jsonWithInvData, err := json.Marshal(&inventory)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	return jsonWithInvData, nil
}
