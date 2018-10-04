package controller

// import (
// 	"encoding/json"
// 	"fmt"
// 	"log"

// 	mongo "github.com/TerrexTech/go-mongoutils/mongo"
// 	"github.com/TerrexTech/go-agg-inventory-cmd/connectDB"
// 	"github.com/TerrexTech/go-agg-inventory-cmd/model"
// 	mgo "github.com/mongodb/mongo-go-driver/mongo"
// 	"github.com/pkg/errors"
// )

// // //Global variable = aggregate version and ID
// // var AggregateVersion = int64(1)
// // var AggregateID = int64(1)

// type InventoryData struct {
// 	Product          *model.Inventory
// 	MongoCollection  *mongo.Collection
// 	SearchField      string
// 	GetValue         interface{}
// 	FilterByName     string
// 	FilterByItemId   int64
// 	GetProductByDate string
// 	StartDate        int64
// 	YesterdayTime    int64
// }

// type searchInventory struct {
// }

// func GetMultipleInv(jsonString []byte) ([]model.Inventory, error) {
// 	inventory := []model.Inventory{}
// 	err := json.Unmarshal(jsonString, &inventory)
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to unmarshal foodItem into Inventory struct")
// 		log.Println(err)
// 		return nil, err
// 	}
// 	return inventory, nil
// }

// func GetMongoCollection() (*connectDB.Db, error) {
// 	Db, err := connectDB.ConfirmDbExists()
// 	if err != nil {
// 		err = errors.Wrap(err, "Mongo client unable to connect")
// 		log.Println(err)
// 	}
// 	return Db, nil
// }

// func GetInventoryFromJSON(jsonString []byte) (*model.Inventory, error) {
// 	inventory := &model.Inventory{}
// 	err := json.Unmarshal(jsonString, inventory)
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to unmarshal foodItem into Inventory struct")
// 		log.Println(err)
// 		return nil, err
// 	}
// 	return inventory, nil
// }

// // func GetInventoryFromJSON(jsonString []byte) (*[]model.Inventory, error) {
// // 	inventory := &[]model.Inventory{}
// // 	err := json.Unmarshal(jsonString, inventory)
// // 	if err != nil {
// // 		err = errors.Wrap(err, "Unable to unmarshal foodItem into Inventory struct")
// // 		log.Println(err)
// // 		return nil, err
// // 	}
// // 	return inventory, nil
// // }

// // func LoadDataForInventory() {
// // 	//Need mgTable collection to connect to Mongo

// // 	// func findProductByName(inventory *model.Inventory, mgTable *mongo.Collection) ([]interface{}, error) {

// // 	findResult, err := mgTable.Find(&model.Inventory{})
// // 	if err != nil {
// // 		err = errors.Wrap(err, "Unable to find product")
// // 		log.Println(err)
// // 		return nil, err
// // 	}
// // 	return findResult, nil

// // }

// func GetMarshal(inventory *[]model.Inventory) ([]byte, error) {
// 	inv, err := json.Marshal(inventory)
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to marshal foodItem into Inventory struct")
// 		log.Println(err)
// 		return nil, err
// 	}
// 	return inv, nil
// }

// func AddProduct(data InventoryData) (*mgo.InsertOneResult, error) {

// 	if data.FilterByName == "item_id" && (data.Product.ItemID == 0) {
// 		return nil, errors.New("Error inserting record. No item_id found")
// 	}

// 	insertResult, err := data.MongoCollection.InsertOne(data.Product)
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to insert event")
// 		log.Println(err)
// 		return nil, err
// 	}

// 	// fmt.Println(insertResult)
// 	return insertResult, nil
// 	// return nil, nil
// }

// func GetFoodProducts(data InventoryData) ([]interface{}, error) {
// 	findResults, err := data.MongoCollection.FindMap(map[string]interface{}{
// 		data.SearchField: map[string]interface{}{
// 			"$eq": data.GetValue,
// 		},
// 	})
// 	if err != nil {
// 		err = errors.Wrap(err, "Error while fetching food product.")
// 		log.Println(err)
// 		return nil, err
// 	}

// 	return findResults, nil
// }

// func UpdateProduct(data InventoryData) (*mgo.UpdateResult, error) {
// 	filter := &model.Inventory{
// 		ItemID: data.FilterByItemId,
// 	}
// 	if data.FilterByItemId == 0 {
// 		return nil, errors.New("item_id cannot be 0")
// 	}

// 	//how to convert struct to map?

// 	update := &map[string]interface{}{
// 		"item_id":      data.Product.ItemID,
// 		"name":         data.Product.Name,
// 		"origin":       data.Product.Origin,
// 		"date_arrived": data.Product.DateArrived,
// 		"device_id":    data.Product.DeviceID,
// 		"price":        data.Product.Price,
// 		"total_weight": data.Product.TotalWeight,
// 		"location":     data.Product.Location,
// 	}

// 	updateResult, err := data.MongoCollection.UpdateMany(filter, update)
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to update event")
// 		log.Println(err)
// 		return nil, err
// 	}
// 	fmt.Println(updateResult)

// 	return updateResult, nil
// }

// func DeleteProduct(data InventoryData) (*mgo.DeleteResult, error) {

// 	if data.FilterByName == "item_id" && (data.Product.ItemID == 0) {
// 		return nil, errors.New("Error deleting product.")
// 	}

// 	if data.FilterByName != "item_id" {
// 		return nil, errors.New("Error deleting product.")
// 	}

// 	deleteResult, err := data.MongoCollection.DeleteMany(&model.Inventory{
// 		ItemID: data.Product.ItemID,
// 	})
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to delete event")
// 		log.Println(err)
// 		return nil, err
// 	}

// 	fmt.Println(deleteResult)

// 	return deleteResult, nil
// }

// func GetProductInRange(data InventoryData) ([]interface{}, error) {
// 	// if data.FilterByName == "expiry_date" && (uuid.UUID{}).String() == "" {
// 	// 	return nil, errors.New("Error Receiving product.")
// 	// }

// 	// if data.FilterByName != "expiry_date" {
// 	// 	return nil, errors.New("Unable to get product.")
// 	// }

// 	log.Println("******", data.StartDate)
// 	findResults, err := data.MongoCollection.FindMap(map[string]interface{}{

// 		"expiry_date": map[string]int64{
// 			"$lt": data.StartDate,
// 		},
// 	})
// 	if err != nil {
// 		err = errors.Wrap(err, "Error while fetching product.")
// 		log.Println(err)
// 		return nil, err
// 	}

// 	return findResults, nil

// }

// // func findProductById(inventory *model.Inventory, mgTable *mongo.Collection) ([]interface{}, error) {

// // 	findResult, err := mgTable.Find(&model.Inventory{
// // 		ItemID: inventory.ItemID,
// // 	})
// // 	if err != nil {
// // 		err = errors.Wrap(err, "Unable to find product")
// // 		log.Println(err)
// // 		return nil, err
// // 	}
// // 	return findResult, nil
// // }

// // func findProductByName(inventory *model.Inventory, mgTable *mongo.Collection) ([]interface{}, error) {

// // 	findResult, err := mgTable.Find(&model.Inventory{
// // 		Name: inventory.Name,
// // 	})
// // 	if err != nil {
// // 		err = errors.Wrap(err, "Unable to find product")
// // 		log.Println(err)
// // 		return nil, err
// // 	}
// // 	return findResult, nil
// // }

// // type TimeRange struct {
// // 	Week  int64 `json:"week,omitempty"`
// // 	Month int64 `json:"month,omitempty"`
// // 	Year  int64 `json:"year,omitempty"`
// // }

// // func GetTimeJSON(jsonString []byte) (*TimeRange, error) {
// // 	var timeRange TimeRange
// // 	err := json.Unmarshal(jsonString, &timeRange)
// // 	if err != nil {
// // 		err = errors.Wrap(err, "Unable to unmarshal foodItem into Inventory struct")
// // 		log.Println(err)
// // 		return nil, err
// // 	}
// // 	return &timeRange, nil
// // }

// // //Of What?
// // func FindByDateArrived(inventory *model.Inventory, specificPeriod string, mgTable *mongo.Collection) ([]interface{}, error) {
// // 	var result []interface{}
// // 	var greaterThanDate time.Time
// // 	var lessThanDate time.Time

// // 	greaterThanDate = time.Now().AddDate()

// // 	switch specificPeriod {
// // 	case "week":
// // 		greaterThanDate = time.Now().AddDate(0, 0, -7)
// // 		lessThanDate = time.Now()
// // 	case "twoWeeks":
// // 		greaterThanDate = time.Now().AddDate(0, 0, -14)
// // 		lessThanDate = time.Now()
// // 	case "oneMonth":
// // 		greaterThanDate = time.Now().AddDate(0, -1, 0)
// // 		lessThanDate = time.Now()
// // 	case "threeMonths":
// // 		greaterThanDate = time.Now().AddDate(0, -3, 0)
// // 		lessThanDate = time.Now()
// // 	case "sixMonths":
// // 		greaterThanDate = time.Now().AddDate(0, -6, 0)
// // 		lessThanDate = time.Now()
// // 	case "oneYear":
// // 		greaterThanDate = time.Now().AddDate(-1, 0, 0)
// // 		lessThanDate = time.Now()
// // 	}

// // 	result, err := mgTable.FindMap(map[string]interface{}{
// // 		"date_arrived": map[string]time.Time{
// // 			"gt": greaterThanDate,
// // 			"lt": lessThanDate,
// // 		},
// // 	})
// // 	if err != nil {
// // 		err = errors.Wrap(err, "Unable to find result for specific dates")
// // 		log.Println(err)
// // 		return nil, err
// // 	}
// // 	return result, nil
// // }

////////////////////////////////////////////////////////////////

////AddProduct---- rough
// log.Println(string(body))
// inventory, err := service.GetInventoryFromJSON(body)
// if err != nil {
// 	err = errors.Wrap(err, "Unable to unmarshal request body into Inventory struct")
// 	log.Println(err)
// 	w.WriteHeader(http.StatusInternalServerError)
// 	return
// }

// mongoColl, err := service.GetMongoCollection()
// if err != nil {
// 	err = errors.Wrap(err, "Unable to connect to Mongo")
// 	log.Println(err)
// 	w.WriteHeader(http.StatusInternalServerError)
// 	return
// }

// _, err = service.AddProduct(service.InventoryData{
// 	Product:         inventory,
// 	MongoCollection: mongoColl.Collection,
// 	FilterByName:    "item_id",
// })
// if err != nil {
// 	err = errors.Wrap(err, "Unable to add product")
// 	log.Println(err)
// 	w.WriteHeader(http.StatusInternalServerError)
// 	return
// }

////Update --- rought
// inventory, err := service.GetInventoryFromJSON(body)
// if err != nil {
// 	err = errors.Wrap(err, "Unable to unmarshal request body into Inventory struct")
// 	log.Println(err)
// 	w.WriteHeader(http.StatusInternalServerError)
// 	return
// }

// mongoColl, err := service.GetMongoCollection()
// if err != nil {
// 	err = errors.Wrap(err, "Unable to connect to Mongo")
// 	log.Println(err)
// 	w.WriteHeader(http.StatusInternalServerError)
// 	return
// }

// _, err = service.UpdateProduct(service.InventoryData{
// 	Product:         inventory,
// 	MongoCollection: mongoColl.Collection,
// 	FilterByItemId:  inventory.ItemID,
// })
// if err != nil {
// 	err = errors.Wrap(err, "Unable to add product")
// 	log.Println(err)
// 	w.WriteHeader(http.StatusInternalServerError)
// 	return
// }

// //Temperary function
// func Insert(w http.ResponseWriter, r *http.Request) {
// 	body, err := ioutil.ReadAll(r.Body)
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to read the request body")
// 		log.Println(err)
// 		w.WriteHeader(http.StatusInternalServerError)
// 		return
// 	}

// 	Db, err := connectDB.ConfirmDbExists()
// 	if err != nil {
// 		err = errors.Wrap(err, "Mongo client unable to connect")
// 		log.Println(err)
// 		w.WriteHeader(http.StatusInternalServerError)
// 		return
// 	}

// 	test, _ := service.GetMultipleInv(body)
// 	for _, v := range test {
// 		t := time.Now().Unix()
// 		v.Timestamp = t
// 		insertResult, err := service.AddProduct(service.InventoryData{
// 			Product:         &v,
// 			MongoCollection: Db.Collection,
// 		})
// 		if err != nil {
// 			err = errors.Wrap(err, "Unable to unmarshal request body into Inventory struct")
// 			log.Println(err)
// 			w.WriteHeader(http.StatusInternalServerError)
// 			return
// 		}

// 		log.Println(insertResult)
// 	}
// inventory, err := service.GetMultipleInv([]byte(mockdata.Testing()))
// if err != nil {
// 	err = errors.Wrap(err, "Unable to unmarshal request body into Inventory struct")
// 	log.Println(err)
// 	return
// }
// log.Println(inventory)

// mongoColl, err := service.GetMongoCollection()
// if err != nil {
// 	err = errors.Wrap(err, "Unable to connect to Mongo")
// 	log.Println(err)
// 	return
// }

// _, err = service.GetFoodProducts(service.InventoryData{
// 	SearchField: string(body),
// })
// if err != nil {
// 	err = errors.Wrap(err, "Unable to find product")
// 	log.Println(err)
// 	return
// }
// }

////Delete ---- rough
// inventory, err := service.GetInventoryFromJSON(body)
// if err != nil {
// 	err = errors.Wrap(err, "Unable to unmarshal request body into Inventory struct")
// 	log.Println(err)
// 	w.WriteHeader(http.StatusInternalServerError)
// 	return
// }

// mongoColl, err := service.GetMongoCollection()
// if err != nil {
// 	err = errors.Wrap(err, "Unable to connect to Mongo")
// 	log.Println(err)
// 	w.WriteHeader(http.StatusInternalServerError)
// 	return
// }

// _, err = service.DeleteProduct(service.InventoryData{
// 	Product:         inventory,
// 	MongoCollection: mongoColl.Collection,
// 	FilterByName:    "item_id",
// })
// if err != nil {
// 	err = errors.Wrap(err, "Unable to add product")
// 	log.Println(err)
// 	w.WriteHeader(http.StatusInternalServerError)
// 	return
// }

// func GetProductRangeHandler(w http.ResponseWriter, r *http.Request) {
// 	body, err := ioutil.ReadAll(r.Body)
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to read the request body")
// 		log.Println(err)
// 		w.WriteHeader(http.StatusInternalServerError)
// 		return
// 	}

// 	inventory, err := service.GetInventoryFromJSON(body)
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to unmarshal request body into Inventory struct")
// 		log.Println(err)
// 		w.WriteHeader(http.StatusInternalServerError)
// 		return
// 	}

// 	log.Println(inventory.ExpiryDate)

// 	st := time.Unix(inventory.ExpiryDate, 0)
// 	startTime := st.AddDate(0, 0, -20)

// 	stTime := startTime.Unix()

// 	t := time.Unix(stTime, 0)
// 	yesterdayTime := t.AddDate(0, 0, -40)
// 	yesTime := yesterdayTime.Unix()

// 	log.Println(startTime)

// 	mongoColl, err := service.GetMongoCollection()
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to connect to Mongo")
// 		log.Println(err)
// 		w.WriteHeader(http.StatusInternalServerError)
// 		return
// 	}

// 	getResults, err := service.GetProductInRange(service.InventoryData{
// 		Product:         inventory,
// 		MongoCollection: mongoColl.Collection,
// 		FilterByName:    "expiry_date",
// 		GetValue:        inventory.ExpiryDate,
// 		StartDate:       stTime,
// 		YesterdayTime:   yesTime,
// 	})
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to add product")
// 		log.Println(err)
// 		w.WriteHeader(http.StatusInternalServerError)
// 		return
// 	}

// 	log.Println(getResults)

// 	// for _, v := range getResults {
// 	// 	marshaledJSON := service.GetMarshal(*model.Inventory(v))
// 	// }

// 	w.WriteHeader(http.StatusOK)
// }
