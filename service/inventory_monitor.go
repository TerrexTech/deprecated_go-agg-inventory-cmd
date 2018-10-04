package service

// import (
// 	"encoding/json"
// 	"fmt"
// 	"io/ioutil"
// 	"log"
// 	"net/http"
// 	"time"

// 	"github.com/TerrexTech/go-agg-inventory-cmd/connectDB"
// 	"github.com/TerrexTech/go-agg-inventory-cmd/model"
// 	"github.com/mongodb/mongo-go-driver/bson"
// 	"github.com/pkg/errors"
// )

// type InvOp interface {
// 	Collection() *mongo.Collection
// 	LoadDataInMongo(w http.ResponseWriter, r *http.Request)
// }

// type InvSearch struct {
// 	EndDate   int64  `bson:"end_date,omitempty" json:"end_date,omitempty"`
// 	StartDate int64  `bson:"start_date,omitempty" json:"start_date,omitempty"`
// 	SearchKey string `bson:"search_key,omitempty" json:"search_key,omitempty"`
// 	SearchVal string `bson:"search_val,omitempty" json:"search_val,omitempty"`
// }

// type InvDashboard struct {
// 	ProdName     string  `bson:"prod_name,omitempty" json:"prod_name,omitempty"`
// 	ProdWeight   float64 `bson:"prod_weight,omitempty" json:"prod_weight,omitempty"`
// 	TotalWeight  float64 `bson:"total_weight,omitempty" json:"total_weight,omitempty"`
// 	SoldWeight   float64 `bson:"sold_weight,omitempty" json:"sold_weight,omitempty"`
// 	WasteWeight  float64 `bson:"waste_weight,omitempty" json:"waste_weight,omitempty"`
// 	ProductSold  int64   `bson:"prod_sold,omitempty" json:"prod_sold,omitempty"`
// 	DonateWeight float64 `bson:"donate_weight,omitempty" json:"donate_weight,omitempty"`
// 	Dates        int64   `bson:"dates,omitempty" json:"dates,omitempty"`
// }

// func LoadDataInMongo(w http.ResponseWriter, r *http.Request) {
// 	if origin := r.Header.Get("Origin"); origin != "" {
// 		w.Header().Set("Access-Control-Allow-Origin", origin)
// 		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
// 		w.Header().Set("Access-Control-Allow-Headers",
// 			"Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
// 	}
// 	// Stop here if its Preflighted OPTIONS request
// 	if r.Method == "OPTIONS" {
// 		return
// 	}

// 	// DB connection
// 	Db, err := connectDB.ConfirmDbExists()
// 	if err != nil {
// 		err = errors.Wrap(err, "Mongo client unable to connect")
// 		log.Println(err)
// 		return
// 	}

// 	inventory := []model.Inventory{}
// 	for i := 0; i < 100; i++ {
// 		inventory = append(inventory, GenerateDataForInv())
// 	}

// 	for _, v := range inventory {
// 		test, err := bson.Marshal(v)
// 		if err != nil {
// 			err = errors.Wrap(err, "Unable to marshal bson - LoadDataInMongo")
// 			log.Println(err)
// 			return
// 		}
// 		log.Println(test)
// 	}

// 	for _, val := range inventory {
// 		log.Println(val.ItemID)
// 		insertResult, err := Db.Collection.InsertOne(val)
// 		if err != nil {
// 			err = errors.Wrap(err, "Unable to insert event")
// 			log.Println(err)
// 			return
// 		}
// 		log.Println(insertResult)
// 	}

// 	_, err = json.Marshal(&inventory)
// 	if err != nil {
// 		log.Println(err)
// 	}
// }

// func SearchOneTime(req []byte) *[]model.Inventory {
// 	Db, err := connectDB.ConfirmDbExists()
// 	if err != nil {
// 		err = errors.Wrap(err, "Mongo client unable to connect")
// 		log.Println(err)
// 		return nil
// 	}

// 	searchInv := InvSearch{}

// 	err = json.Unmarshal(req, &searchInv)
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to unmarshal - SearchOneTime")
// 		log.Println(err)
// 		return nil
// 	}

// 	log.Println(searchInv, "************************")

// 	findResults, err := Db.Collection.Find(map[string]interface{}{

// 		"timestamp": map[string]int64{
// 			"$lt": searchInv.EndDate,
// 		},
// 	})
// 	if err != nil {
// 		err = errors.Wrap(err, "Error while fetching product.")
// 		log.Println(err)
// 		return nil
// 	}
// 	log.Println(findResults)

// 	inventory := []model.Inventory{}

// 	for _, v := range findResults {
// 		resultInv := v.(*model.Inventory)
// 		inventory = append(inventory, *resultInv)
// 	}

// 	return &inventory
// }

// func LoadInventoryTable(w http.ResponseWriter, r *http.Request) {

// 	if origin := r.Header.Get("Origin"); origin != "" {
// 		w.Header().Set("Access-Control-Allow-Origin", origin)
// 		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
// 		w.Header().Set("Access-Control-Allow-Headers",
// 			"Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
// 	}
// 	// Stop here if its Preflighted OPTIONS request
// 	if r.Method == "OPTIONS" {
// 		return
// 	}

// 	body, err := ioutil.ReadAll(r.Body)
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to read the request body")
// 		log.Println(err)
// 		return
// 	}

// 	// log.Println(string(body))

// 	// inventory := SearchBtwTimeRange(body) //Just need max time
// 	inventory := SearchOneTime(body)

// 	totalResult, err := json.Marshal(&inventory)
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to create response body")
// 		log.Println(err)
// 		return
// 	}
// 	w.Write(totalResult)
// }

// func SearchInvTable(w http.ResponseWriter, r *http.Request) {

// 	if origin := r.Header.Get("Origin"); origin != "" {
// 		w.Header().Set("Access-Control-Allow-Origin", origin)
// 		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
// 		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
// 	}
// 	// Stop here if its Preflighted OPTIONS request
// 	if r.Method == "OPTIONS" {
// 		return
// 	}

// 	body, err := ioutil.ReadAll(r.Body)
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to read the request body")
// 		log.Println(err)
// 		return
// 	}

// 	//DB connection
// 	Db, err := connectDB.ConfirmDbExists()
// 	if err != nil {
// 		err = errors.Wrap(err, "Mongo client unable to connect")
// 		log.Println(err)
// 		return
// 	}

// 	// log.Println(string(body))

// 	//Convert body of type []byte into type InvSearch
// 	search := InvSearch{}

// 	err = json.Unmarshal(body, &search)
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to unmarshal foodItem into Inventory struct")
// 		log.Println(err)
// 		return
// 	}

// 	log.Println(search)

// 	findResults, err := Db.Collection.Find(map[string]interface{}{

// 		search.SearchKey: map[string]*string{
// 			"$eq": &search.SearchVal,
// 		},
// 	})
// 	if err != nil {
// 		err = errors.Wrap(err, "Error while fetching product.")
// 		log.Println(err)
// 		return
// 	}

// 	inventory := []model.Inventory{}

// 	for _, v := range findResults {
// 		resultInv := v.(*model.Inventory)
// 		inventory = append(inventory, *resultInv)
// 	}

// 	searchResult, err := json.Marshal(inventory)
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to create response body")
// 		log.Println(err)
// 		return
// 	}
// 	w.Write(searchResult)

// }

// func AddInventory(w http.ResponseWriter, r *http.Request) {
// 	if origin := r.Header.Get("Origin"); origin != "" {
// 		w.Header().Set("Access-Control-Allow-Origin", origin)
// 		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
// 		w.Header().Set("Access-Control-Allow-Headers",
// 			"Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
// 	}
// 	// Stop here if its Preflighted OPTIONS request
// 	if r.Method == "OPTIONS" {
// 		return
// 	}

// 	body, err := ioutil.ReadAll(r.Body)
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to read the request body")
// 		log.Println(err)
// 		return
// 	}

// 	//DB connection
// 	Db, err := connectDB.ConfirmDbExists()
// 	if err != nil {
// 		err = errors.Wrap(err, "Mongo client unable to connect")
// 		log.Println(err)
// 		return
// 	}

// 	log.Println(string(body))

// 	//Convert body of type []byte into type []model.Inventory{}
// 	inventory := model.Inventory{}
// 	err = json.Unmarshal(body, &inventory)
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to unmarshal foodItem into Inventory struct")
// 		log.Println(err)
// 		return
// 	}

// 	if inventory.ItemID.String() != "" { //need to change this
// 		inventory.Timestamp = time.Now().Unix()
// 		log.Println(inventory.ItemID)
// 		insertResult, err := Db.Collection.InsertOne(inventory)
// 		if err != nil {
// 			err = errors.Wrap(err, "Unable to insert - AddInventory")
// 			log.Println(err)
// 			return
// 		}
// 		log.Println(insertResult)
// 	}

// 	// //Convert body of type []byte into type []model.Inventory{}
// 	// inventory := []model.Inventory{}
// 	// err = json.Unmarshal(body, &inventory)
// 	// if err != nil {
// 	// 	err = errors.Wrap(err, "Unable to unmarshal foodItem into Inventory struct")
// 	// 	log.Println(err)
// 	// 	return
// 	// }

// 	// for _, val := range inventory {
// 	// 	if val.ItemID.String() != "" { //need to change this
// 	// 		val.Timestamp = time.Now().Unix()
// 	// 		log.Println(val.ItemID)
// 	// 		insertResult, err := Db.Collection.InsertOne(val)
// 	// 		if err != nil {
// 	// 			err = errors.Wrap(err, "Unable to insert event")
// 	// 			log.Println(err)
// 	// 			return
// 	// 		}
// 	// 		log.Println(insertResult)
// 	// 	}
// 	// }
// }

// func UpdateInventory(w http.ResponseWriter, r *http.Request) {

// 	if origin := r.Header.Get("Origin"); origin != "" {
// 		w.Header().Set("Access-Control-Allow-Origin", origin)
// 		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
// 		w.Header().Set("Access-Control-Allow-Headers",
// 			"Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
// 	}
// 	// Stop here if its Preflighted OPTIONS request
// 	if r.Method == "OPTIONS" {
// 		return
// 	}

// 	body, err := ioutil.ReadAll(r.Body)
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to read the request body")
// 		log.Println(err)
// 		return
// 	}

// 	Db, err := connectDB.ConfirmDbExists()
// 	if err != nil {
// 		err = errors.Wrap(err, "Mongo client unable to connect")
// 		log.Println(err)
// 		return
// 	}

// 	inventory := &model.Inventory{}
// 	//Convert body of type []byte into type []model.Inventory{}
// 	err = json.Unmarshal(body, inventory)
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to unmarshal foodItem into Inventory struct")
// 		log.Println(err)
// 		return
// 	}

// 	//Filter required for Update
// 	filter := &model.Inventory{
// 		ItemID: inventory.ItemID,
// 	}

// 	//Confirm that fields are not empty
// 	if inventory.ItemID.String() == "" {
// 		log.Println("UUID is empty")
// 		return
// 	}

// 	if inventory.Name == "" {
// 		log.Println("Name is empty")
// 		return
// 	}

// 	if inventory.Origin == "" {
// 		log.Println("Origin is empty")
// 		return
// 	}

// 	if inventory.DateArrived == 0 {
// 		log.Println("Date arrived is empty")
// 		return
// 	}

// 	if inventory.DeviceID.String() == "" {
// 		log.Println("DeviceID is empty")
// 		return
// 	}

// 	if inventory.Price == 0 {
// 		log.Println("Price is empty")
// 		return
// 	}

// 	if inventory.TotalWeight == 0 {
// 		log.Println("Total weight is empty")
// 		return
// 	}

// 	if inventory.Location == "" {
// 		log.Println("Location is empty")
// 		return
// 	}

// 	//Adding the timestamp
// 	nowTime := time.Now().Unix()
// 	inventory.Timestamp = nowTime

// 	update := &map[string]interface{}{
// 		"item_id":      inventory.ItemID,
// 		"name":         inventory.Name,
// 		"origin":       inventory.Origin,
// 		"date_arrived": inventory.DateArrived,
// 		"device_id":    inventory.DeviceID,
// 		"price":        inventory.Price,
// 		"total_weight": inventory.TotalWeight,
// 		"location":     inventory.Location,
// 	}

// 	updateResult, err := Db.Collection.UpdateMany(filter, update)
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to update event")
// 		return
// 	}
// 	fmt.Println(updateResult)

// 	if updateResult.ModifiedCount > 0 {
// 		w.Write([]byte(("Updated: ") + string(updateResult.ModifiedCount)))
// 	}
// }

// func DeleteInventory(w http.ResponseWriter, r *http.Request) {
// 	if origin := r.Header.Get("Origin"); origin != "" {
// 		w.Header().Set("Access-Control-Allow-Origin", origin)
// 		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
// 		w.Header().Set("Access-Control-Allow-Headers",
// 			"Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
// 	}
// 	// Stop here if its Preflighted OPTIONS request
// 	if r.Method == "OPTIONS" {
// 		return
// 	}

// 	var delCount int64

// 	body, err := ioutil.ReadAll(r.Body)
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to read the request body")
// 		log.Println(err)
// 		return
// 	}

// 	Db, err := connectDB.ConfirmDbExists()
// 	if err != nil {
// 		err = errors.Wrap(err, "Mongo client unable to connect")
// 		log.Println(err)
// 		return
// 	}

// 	log.Println(string(body))

// 	inventory := []model.Inventory{}
// 	//Convert body of type []byte into type []model.Inventory{}
// 	err = json.Unmarshal(body, &inventory)
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to unmarshal - DeleteInventory")
// 		log.Println(err)
// 		return
// 	}

// 	for _, inVal := range inventory {
// 		// log.Println(inVal)
// 		// if inVal.ItemID == string() {
// 		// 	log.Println("ItemID not found")
// 		// 	return
// 		// }

// 		deleteResult, err := Db.Collection.DeleteMany(&model.Inventory{
// 			ItemID: inVal.ItemID,
// 		})
// 		if err != nil {
// 			err = errors.Wrap(err, "Unable to delete event")
// 			log.Println(err)
// 			return
// 		}
// 		if deleteResult.DeletedCount > 0 {
// 			delCount = delCount + 1
// 		}
// 	}
// 	w.Write([]byte(("Deleted: ") + string(delCount)))
// }

// //need end and start time (start in days)
// func TimeSearchInTable(w http.ResponseWriter, r *http.Request) {
// 	if origin := r.Header.Get("Origin"); origin != "" {
// 		w.Header().Set("Access-Control-Allow-Origin", origin)
// 		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
// 		w.Header().Set("Access-Control-Allow-Headers",
// 			"Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
// 	}
// 	// Stop here if its Preflighted OPTIONS request
// 	if r.Method == "OPTIONS" {
// 		return
// 	}

// 	body, err := ioutil.ReadAll(r.Body)
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to read the request body")
// 		log.Println(err)
// 		w.WriteHeader(http.StatusInternalServerError)
// 		return
// 	}

// 	inventory := SearchBtwTimeRange(body) //Just need max time

// 	if len(*inventory) > 0 {
// 		invJSON, err := json.Marshal(inventory)
// 		if err != nil {
// 			err = errors.Wrap(err, "Unable to marshal foodItem into Inventory struct")
// 			log.Println(err)
// 			w.WriteHeader(http.StatusInternalServerError)
// 			return
// 		}
// 		w.Write(invJSON)
// 	}
// }

// //find results for timestamp field within a specified time range
// func SearchBtwTimeRange(req []byte) *[]model.Inventory {
// 	Db, err := connectDB.ConfirmDbExists()
// 	if err != nil {
// 		err = errors.Wrap(err, "Mongo client unable to connect")
// 		log.Println(err)
// 		return nil
// 	}
// 	log.Println(string(req))
// 	searchInv := []InvSearch{}

// 	var findResults []interface{}

// 	err = json.Unmarshal(req, &searchInv)
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to unmarshal foodItem into Inventory struct")
// 		log.Println(err)
// 		return nil
// 	}

// 	for _, searchVal := range searchInv {
// 		findResults, err = Db.Collection.Find(map[string]interface{}{

// 			"timestamp": map[string]int64{
// 				"$lt": searchVal.EndDate,
// 				"$gt": searchVal.StartDate,
// 			},
// 		})
// 		if err != nil {
// 			err = errors.Wrap(err, "Error while fetching product.")
// 			log.Println(err)
// 			return nil
// 		}
// 	}

// 	inventory := []model.Inventory{}

// 	for _, v := range findResults {
// 		resultInv := v.(*model.Inventory)
// 		inventory = append(inventory, *resultInv)
// 	}
// 	return &inventory
// }

// func TotalWeightSoldWasteDonatePerDay(w http.ResponseWriter, r *http.Request) {
// 	if origin := r.Header.Get("Origin"); origin != "" {
// 		w.Header().Set("Access-Control-Allow-Origin", origin)
// 		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
// 		w.Header().Set("Access-Control-Allow-Headers",
// 			"Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
// 	}
// 	// Stop here if its Preflighted OPTIONS request
// 	if r.Method == "OPTIONS" {
// 		return
// 	}
// 	var totalWeight float64
// 	var soldWeight float64
// 	var wasteWeight float64
// 	var donateWeight float64

// 	var tweight []float64
// 	var sweight []float64
// 	var wweight []float64
// 	var dweight []float64

// 	//Get timestamp from frontend
// 	body, err := ioutil.ReadAll(r.Body)
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to read the request body")
// 		log.Println(err)
// 		w.WriteHeader(http.StatusInternalServerError)
// 		return
// 	}

// 	//Get list of inventory from SearchDay
// 	inventory := *SearchBtwTimeRange(body) //Just need max time
// 	log.Println(inventory)

// 	// log.Printf("%+v", inventory[0])
// 	if len(inventory) == 0 {
// 		log.Println(errors.New("Unable to get anything back from SearchWithTime function"))
// 		// w.WriteHeader(http.StatusInternalServerError)
// 		return
// 	}

// 	for _, v := range inventory {
// 		totalWeight = v.TotalWeight + totalWeight
// 		tweight = append(tweight, totalWeight)

// 		log.Println(v.SoldWeight, "&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
// 		soldWeight = v.SoldWeight + soldWeight
// 		sweight = append(sweight, soldWeight)

// 		wasteWeight = v.WasteWeight + wasteWeight
// 		wweight = append(wweight, wasteWeight)

// 		donateWeight = v.DonateWeight + donateWeight
// 		dweight = append(dweight, donateWeight)
// 	}

// 	invSearch := []InvSearch{}
// 	err = json.Unmarshal(body, &invSearch)
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to Unmarshal timestamp from body - TwSaleWasteDonate")
// 		log.Println(err)
// 		return
// 	}

// 	log.Println(invSearch)
// 	var totalResult []byte

// 	dash := make(map[int]InvDashboard)
// 	// dash := []InvDashboard{}
// 	for i, v := range invSearch {
// 		log.Println(sweight[i])

// 		dash[i] = InvDashboard{
// 			TotalWeight:  tweight[i],
// 			SoldWeight:   sweight[i],
// 			WasteWeight:  wweight[i],
// 			DonateWeight: dweight[i],
// 			Dates:        v.StartDate,
// 		}

// 		log.Println(dash)
// 	}

// 	totalResult, err = json.Marshal(dash)
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to create response body")
// 		log.Println(err)
// 		return
// 	}
// 	w.Write(totalResult)
// }

// func ProdSoldPerHour(w http.ResponseWriter, r *http.Request) {
// 	if origin := r.Header.Get("Origin"); origin != "" {
// 		w.Header().Set("Access-Control-Allow-Origin", origin)
// 		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
// 		w.Header().Set("Access-Control-Allow-Headers",
// 			"Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
// 	}
// 	// Stop here if its Preflighted OPTIONS request
// 	if r.Method == "OPTIONS" {
// 		return
// 	}

// 	var soldWeight float64
// 	var sweight []float64

// 	body, err := ioutil.ReadAll(r.Body)
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to read the request body")
// 		log.Println(err)
// 		w.WriteHeader(http.StatusInternalServerError)
// 		return
// 	}

// 	inventory := *SearchBtwTimeRange(body) //Just need max time

// 	if len(inventory) == 0 {
// 		log.Println(errors.New("Unable to get anything back from SearchBtwTimeRange function"))
// 		return
// 	}

// 	for _, v := range inventory {
// 		soldWeight = v.SoldWeight + soldWeight
// 		sweight = append(sweight, soldWeight)

// 		log.Println(sweight, "********************")
// 	}

// 	invSearch := []InvSearch{}
// 	err = json.Unmarshal(body, &invSearch)
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to Unmarshal timestamp from body - TwSaleWasteDonate")
// 		log.Println(err)
// 		return
// 	}

// 	var totalResult []byte

// 	dash := make(map[int]InvDashboard)
// 	for i, v := range invSearch {

// 		dash[i] = InvDashboard{
// 			SoldWeight: sweight[i],
// 			Dates:      v.StartDate,
// 		}

// 		log.Println(dash)
// 	}

// 	totalResult, err = json.Marshal(dash)
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to create response body")
// 		log.Println(err)
// 		return
// 	}
// 	w.Write(totalResult)
// }

// // type bla struct {
// // 	ID string `bson:"_id", json:"_id"`,
// // 	Total float32 `bson:"total", json:"total"`
// // }

// //Need end time
// func DistributionByWeight(w http.ResponseWriter, r *http.Request) {

// 	if origin := r.Header.Get("Origin"); origin != "" {
// 		w.Header().Set("Access-Control-Allow-Origin", origin)
// 		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
// 		w.Header().Set("Access-Control-Allow-Headers",
// 			"Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
// 	}
// 	// Stop here if its Preflighted OPTIONS request
// 	if r.Method == "OPTIONS" {
// 		return
// 	}

// 	Db, err := connectDB.ConfirmDbExists()
// 	if err != nil {
// 		err = errors.Wrap(err, "Mongo client unable to connect")
// 		log.Println(err)
// 		w.WriteHeader(http.StatusInternalServerError)
// 		return
// 	}

// 	pipeline := bson.NewArray(
// 		bson.VC.Document(
// 			bson.NewDocument(
// 				bson.EC.SubDocumentFromElements(
// 					"$group",
// 					bson.EC.String("_id", "$name"),
// 					bson.EC.SubDocumentFromElements(
// 						"total",
// 						bson.EC.String("$sum", "$total_weight"),
// 					),
// 				),
// 			),
// 		),
// 	)
// 	aggResults, err := Db.Collection.Aggregate(pipeline)
// 	if err != nil {
// 		log.Fatalln(err)
// 	}
// 	// log.Println(aggResults)

// 	dist := []InvDashboard{}

// 	for _, v := range aggResults {
// 		value := v.(map[string]interface{})
// 		strValue := value["_id"].(string)
// 		secValue := value["total"].(float64)
// 		dist = append(dist, InvDashboard{
// 			ProdName:   strValue,
// 			ProdWeight: secValue,
// 		})
// 	}

// 	DistWeightByte, err := json.Marshal(&dist)
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to marshal distribution")
// 		log.Println(err)
// 		w.WriteHeader(http.StatusInternalServerError)
// 		return
// 	}

// 	w.Write(DistWeightByte)
// 	w.WriteHeader(http.StatusOK)
// }

// //find results for timestamp field within a specified time range
// // func SearchByHours(req []byte) *[]model.Inventory {
// // 	Db, err := connectDB.ConfirmDbExists()
// // 	if err != nil {
// // 		err = errors.Wrap(err, "Mongo client unable to connect")
// // 		log.Println(err)
// // 		return nil
// // 	}

// // 	searchInv := []InvSearch{}
// // 	var findResults []interface{}
// // 	err = json.Unmarshal(req, &searchInv)
// // 	if err != nil {
// // 		err = errors.Wrap(err, "Unable to unmarshal foodItem into Inventory struct")
// // 		log.Println(err)
// // 		return nil
// // 	}
// // 	for _, searchVal := range searchInv {
// // 		if searchVal.TimeInHours != 0 {
// // 			startTime := searchVal.MaxTime - 3600

// // 			findResults, err = Db.Collection.Find(map[string]interface{}{

// // 				"timestamp": map[string]*int64{
// // 					"$lt": &searchVal.MaxTime,
// // 					"$gt": &startTime,
// // 				},
// // 			})
// // 		}
// // 		if err != nil {
// // 			err = errors.Wrap(err, "Error while fetching product.")
// // 			log.Println(err)
// // 			return nil
// // 		}
// // 	}

// // 	inventory := []model.Inventory{}

// // 	for _, v := range findResults {
// // 		resultInv := v.(*model.Inventory)
// // 		inventory = append(inventory, *resultInv)
// // 	}

// // 	return &inventory

// // 	// log.Println(len(findResults))
// // 	// return findResults
// // }

// //need end and start time (start period has to be in number of days)
// // func GetInvForToday(w http.ResponseWriter, r *http.Request) {
// // 	body, err := ioutil.ReadAll(r.Body)
// // 	if err != nil {
// // 		err = errors.Wrap(err, "Unable to read the request body")
// // 		log.Println(err)
// // 		w.WriteHeader(http.StatusInternalServerError)
// // 		return
// // 	}

// // 	Db, err := connectDB.ConfirmDbExists()
// // 	if err != nil {
// // 		err = errors.Wrap(err, "Mongo client unable to connect")
// // 		log.Println(err)
// // 		w.WriteHeader(http.StatusInternalServerError)
// // 		return
// // 	}

// // 	searchInv := &InvSearch{}
// // 	err = json.Unmarshal(body, searchInv)
// // 	if err != nil {
// // 		err = errors.Wrap(err, "Unable to unmarshal foodItem into Inventory struct")
// // 		log.Println(err)
// // 		w.WriteHeader(http.StatusInternalServerError)
// // 		return
// // 	}

// // 	endTime := time.Unix(searchInv.MaxTime, 0)
// // 	startTime := endTime.AddDate(0, 0, -(int(searchInv.TimePeriodInDays))).Unix()

// // 	findResults, err := Db.Collection.Find(map[string]interface{}{

// // 		"timestamp": map[string]*int64{
// // 			"$lt": &searchInv.MaxTime,
// // 			"$gt": &startTime,
// // 		},
// // 	})
// // 	if err != nil {
// // 		err = errors.Wrap(err, "Error while fetching product.")
// // 		log.Println(err)
// // 		w.WriteHeader(http.StatusInternalServerError)
// // 		return
// // 	}

// // 	log.Println(len(findResults))
// // 	resultCount := strconv.Itoa(len(findResults))
// // 	w.Write([]byte(resultCount))
// // 	w.WriteHeader(http.StatusOK)

// // 	// pipeline := bson.NewArray(
// // 	// 	bson.VC.DocumentFromElements(
// // 	// 		bson.EC.SubDocumentFromElements(
// // 	// 			"$match",
// // 	// 			bson.EC.SubDocumentFromElements(
// // 	// 				"timestamp",
// // 	// 				bson.EC.Int64("$gte", startTime),
// // 	// 				bson.EC.Int64("$lte", searchInv.MaxTime),
// // 	// 			),
// // 	// 		),
// // 	// 	),
// // 	// 	bson.VC.DocumentFromElements(
// // 	// 		bson.EC.SubDocumentFromElements(
// // 	// 			"$group",
// // 	// 			bson.EC.SubDocumentFromElements(
// // 	// 				"_id",
// // 	// 				bson.EC.String("_id", nil),
// // 	// 			),
// // 	// 			bson.EC.SubDocumentFromElements(
// // 	// 				"count",
// // 	// 				bson.EC.count("count", 1),
// // 	// 			),
// // 	// 		),
// // 	// 	),
// // 	// )
// // 	// aggResults, err := Db.Collection.Aggregate(pipeline)
// // 	// if err != nil {
// // 	// 	log.Fatalln(err)
// // 	// }
// // 	// log.Println(len(aggResults))
// // 	// for _, r := range aggResults {
// // 	// 	// log.Println(r.(*item))
// // 	// }
// // }

// // //need end time
// // func TotalInventory(w http.ResponseWriter, r *http.Request) {
// // 	body, err := ioutil.ReadAll(r.Body)
// // 	if err != nil {
// // 		err = errors.Wrap(err, "Unable to read the request body")
// // 		log.Println(err)
// // 		w.WriteHeader(http.StatusInternalServerError)
// // 		return
// // 	}

// // 	Db, err := connectDB.ConfirmDbExists()
// // 	if err != nil {
// // 		err = errors.Wrap(err, "Mongo client unable to connect")
// // 		log.Println(err)
// // 		w.WriteHeader(http.StatusInternalServerError)
// // 		return
// // 	}

// // 	searchInv := &InvSearch{}
// // 	err = json.Unmarshal(body, searchInv)
// // 	if err != nil {
// // 		err = errors.Wrap(err, "Unable to unmarshal foodItem into Inventory struct")
// // 		log.Println(err)
// // 		w.WriteHeader(http.StatusInternalServerError)
// // 		return
// // 	}

// // 	findResults, err := Db.Collection.Find(map[string]interface{}{

// // 		"timestamp": map[string]*int64{
// // 			"$lt": &searchInv.MaxTime,
// // 		},
// // 	})
// // 	if err != nil {
// // 		err = errors.Wrap(err, "Error while fetching product.")
// // 		log.Println(err)
// // 		w.WriteHeader(http.StatusInternalServerError)
// // 		return
// // 	}

// // 	resultCount := strconv.Itoa(len(findResults))
// // 	w.Write([]byte(resultCount))
// // 	w.WriteHeader(http.StatusOK)
// // }

// // //need to think about how to show it in per hour - require start and end time
// // func AvgInventoryPerHour(w http.ResponseWriter, r *http.Request) {
// // 	body, err := ioutil.ReadAll(r.Body)
// // 	if err != nil {
// // 		err = errors.Wrap(err, "Unable to read the request body")
// // 		log.Println(err)
// // 		w.WriteHeader(http.StatusInternalServerError)
// // 		return
// // 	}

// // 	Db, err := connectDB.ConfirmDbExists()
// // 	if err != nil {
// // 		err = errors.Wrap(err, "Mongo client unable to connect")
// // 		log.Println(err)
// // 		w.WriteHeader(http.StatusInternalServerError)
// // 		return
// // 	}

// // 	searchInv := &InvSearch{}
// // 	err = json.Unmarshal(body, searchInv)
// // 	if err != nil {
// // 		err = errors.Wrap(err, "Unable to unmarshal foodItem into Inventory struct")
// // 		log.Println(err)
// // 		w.WriteHeader(http.StatusInternalServerError)
// // 		return
// // 	}

// // 	startTime := searchInv.MaxTime - 3600

// // 	//here we need to send one hour, weekly, monthly

// // 	findResults, err := Db.Collection.Find(map[string]interface{}{

// // 		"timestamp": map[string]*int64{
// // 			"$lte": &searchInv.MaxTime,
// // 			"$gte": &startTime,
// // 		},
// // 	})
// // 	if err != nil {
// // 		err = errors.Wrap(err, "Error while fetching product.")
// // 		log.Println(err)
// // 		w.WriteHeader(http.StatusInternalServerError)
// // 		return
// // 	}

// // 	resultCount := strconv.Itoa(len(findResults))
// // 	w.Write([]byte(resultCount))
// // 	w.WriteHeader(http.StatusOK)
// // }

// // // func TotalWeightSoldPerFruit(w http.ResponseWriter, r *http.Request) {
// // // 	body, err := ioutil.ReadAll(r.Body)
// // // 	if err != nil {
// // // 		err = errors.Wrap(err, "Unable to read the request body")
// // // 		log.Println(err)
// // // 		w.WriteHeader(http.StatusInternalServerError)
// // // 		return
// // // 	}

// // // 	Db, err := connectDB.ConfirmDbExists()
// // // 	if err != nil {
// // // 		err = errors.Wrap(err, "Mongo client unable to connect")
// // // 		log.Println(err)
// // // 		w.WriteHeader(http.StatusInternalServerError)
// // // 		return
// // // 	}

// // // 	searchInv := &InvSearch{}
// // // 	err = json.Unmarshal(body, searchInv)
// // // 	if err != nil {
// // // 		err = errors.Wrap(err, "Unable to unmarshal foodItem into Inventory struct")
// // // 		log.Println(err)
// // // 		w.WriteHeader(http.StatusInternalServerError)
// // // 		return
// // // 	}

// // // 	startTime := searchInv.MaxTime - 3600

// // // 	//here we need to send one hour, weekly, monthly

// // // 	findResults, err := Db.Collection.Find(map[string]interface{}{

// // // 		"timestamp": map[string]*int64{
// // // 			"$lte": &searchInv.MaxTime,
// // // 			"$gte": &startTime,
// // // 		},
// // // 	})
// // // 	if err != nil {
// // // 		err = errors.Wrap(err, "Error while fetching product.")
// // // 		log.Println(err)
// // // 		w.WriteHeader(http.StatusInternalServerError)
// // // 		return
// // // 	}
// // // 	resultCount := strconv.Itoa(len(findResults))
// // // 	w.Write([]byte(resultCount))
// // // 	w.WriteHeader(http.StatusOK)
// // // }
