package service

import (
	ctx "context"
	"testing"
	"time"

	"github.com/bhupeshbhatia/go-agg-inventory-v2/mockdata"

	mongo "github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/bhupeshbhatia/go-agg-inventory-v2/model"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestBooks(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Books Suite")
}

// newTimeoutContext creates a new WithTimeout context with specified timeout.
func newTimeoutContext(timeout uint32) (ctx.Context, ctx.CancelFunc) {
	return ctx.WithTimeout(
		ctx.Background(),
		time.Duration(timeout)*time.Millisecond,
	)
}

var _ = Describe("Mongo service test", func() {
	var (
		// jsonString string
		mgTable         *mongo.Collection
		client          mongo.Client
		resourceTimeout uint32
		testDatabase    string
		clientConfig    mongo.ClientConfig
		c               *mongo.Collection
	)

	testDatabase = "rns_test"
	resourceTimeout = 3000

	dropTestDatabase := func() {
		client, err := mongo.NewClient(clientConfig)
		Expect(err).ToNot(HaveOccurred())

		dbCtx, dbCancel := newTimeoutContext(resourceTimeout)
		err = client.Database(testDatabase).Drop(dbCtx)
		dbCancel()
		Expect(err).ToNot(HaveOccurred())

		err = client.Disconnect()
		Expect(err).ToNot(HaveOccurred())
	}

	BeforeEach(func() {
		clientConfig = mongo.ClientConfig{
			Hosts:               []string{"127.0.0.1:27017"},
			Username:            "root",
			Password:            "root",
			TimeoutMilliseconds: 5000,
		}
		client, err := mongo.NewClient(clientConfig)
		Expect(err).ToNot(HaveOccurred())

		conn := &mongo.ConnectionConfig{
			Client:  client,
			Timeout: 1000,
		}

		c = &mongo.Collection{
			Connection:   conn,
			Name:         "mtest",
			Database:     testDatabase,
			SchemaStruct: &model.Inventory{},
		}

		dropTestDatabase()
		mgTable, err = mongo.EnsureCollection(c)
		Expect(err).ToNot(HaveOccurred())

	})

	AfterEach(func() {

		err := client.Disconnect()
		Expect(err).ToNot(HaveOccurred())
	})

	It("Test insert one document", func() {
		inventory, err := GetInventoryJSON([]byte(mockdata.JsonForAddProduct()))
		Expect(err).ToNot(HaveOccurred())

		insertResult, err := AddFood(InventoryData{
			Product:    inventory,
			MongoTable: mgTable,
		})
		Expect(err).ToNot(HaveOccurred())

		var checkField = insertResult

		findResult, err := GetFoodProducts(InventoryData{inventory, mgTable, "_id", checkField.InsertedID, "", 0})
		// log.Println("==========================")
		// for _, v := range findResult {
		// 	log.Println(v.(*model.Inventory))
		// }
		Expect(err).ToNot(HaveOccurred())
		for _, v := range findResult {
			Expect(v.(*model.Inventory)).To(Equal(inventory))
		}
	})

	It("Document not inserted when data is empty", func() {
		inventory, err := GetInventoryJSON([]byte(mockdata.JsonAddWithoutID()))
		Expect(err).ToNot(HaveOccurred())

		_, err = AddFood(InventoryData{
			Product:     inventory,
			MongoTable:  mgTable,
			FilterName:  "Fruit_ID",
			FilterValue: inventory.FruitID,
		})
		Expect(err).To(HaveOccurred())
	})

	It("Empty slice is returned when searchField is empty", func() {
		inventory, err := GetInventoryJSON([]byte(mockdata.JsonForAddProduct()))
		Expect(err).ToNot(HaveOccurred())

		insertResult, err := AddFood(InventoryData{
			Product:    inventory,
			MongoTable: mgTable,
		})
		Expect(err).ToNot(HaveOccurred())
		var checkField = insertResult

		findResult, err := GetFoodProducts(InventoryData{inventory, mgTable, "", checkField.InsertedID, "", 0})
		Expect(err).ToNot(HaveOccurred())
		Expect(len(findResult)).To(Equal(0))
	})

	It("Error is returned when search value is empty", func() {
		inventory, err := GetInventoryJSON([]byte(mockdata.JsonForAddProduct()))
		Expect(err).ToNot(HaveOccurred())

		_, err = AddFood(InventoryData{
			Product:    inventory,
			MongoTable: mgTable,
		})
		Expect(err).ToNot(HaveOccurred())
		// var checkField = insertResult

		findResult, err := GetFoodProducts(InventoryData{inventory, mgTable, "_id", "", "", 0})
		Expect(err).ToNot(HaveOccurred())
		Expect(len(findResult)).To(Equal(0))
	})

	It("Should update inventory", func() {
		inventory, err := GetInventoryJSON([]byte(mockdata.JsonForAddProduct()))
		Expect(err).ToNot(HaveOccurred())

		_, err = AddFood(InventoryData{
			Product:    inventory,
			MongoTable: mgTable,
		})
		Expect(err).ToNot(HaveOccurred())

		inventory, err = GetInventoryJSON([]byte(mockdata.JsonForUpdateProduct()))
		Expect(err).ToNot(HaveOccurred())

		updateResult, err := UpdateAgg(InventoryData{
			Product:     inventory,
			MongoTable:  mgTable,
			FilterName:  "Fruit_ID",
			FilterValue: inventory.FruitID,
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(updateResult.MatchedCount).To(Equal(int64(1)))
		Expect(updateResult.ModifiedCount).To(Equal(int64(1)))
	})

	It("Should not update inventory", func() {
		inventory, err := GetInventoryJSON([]byte(mockdata.JsonForAddProduct()))
		Expect(err).ToNot(HaveOccurred())

		_, err = AddFood(InventoryData{
			Product:    inventory,
			MongoTable: mgTable,
		})
		Expect(err).ToNot(HaveOccurred())

		inventory, err = GetInventoryJSON([]byte(mockdata.JsonEmptyUpdateProduct()))
		Expect(err).ToNot(HaveOccurred())

		_, err = UpdateAgg(InventoryData{
			Product:     inventory,
			MongoTable:  mgTable,
			FilterName:  "Fruit_ID",
			FilterValue: inventory.FruitID,
		})
		Expect(err).To(HaveOccurred())
	})

	It("Should delete record", func() {
		inventory, err := GetInventoryJSON([]byte(mockdata.JsonForAddProduct()))
		Expect(err).ToNot(HaveOccurred())

		_, err = AddFood(InventoryData{
			Product:    inventory,
			MongoTable: mgTable,
		})
		Expect(err).ToNot(HaveOccurred())

		inventory, err = GetInventoryJSON([]byte(mockdata.JsonDeleteProduct()))
		Expect(err).ToNot(HaveOccurred())

		deleteResult, err := DeleteAgg(InventoryData{
			Product:     inventory,
			MongoTable:  mgTable,
			FilterName:  "Fruit_ID",
			FilterValue: inventory.FruitID,
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(deleteResult.DeletedCount).To(Equal(int64(1)))
	})

	It("Should not delete if Fruit_ID does not match", func() {
		inventory, err := GetInventoryJSON([]byte(mockdata.JsonForAddProduct()))
		Expect(err).ToNot(HaveOccurred())

		_, err = AddFood(InventoryData{
			Product:    inventory,
			MongoTable: mgTable,
		})
		Expect(err).ToNot(HaveOccurred())

		inventory, err = GetInventoryJSON([]byte(mockdata.JsonDelWithoutFruitID()))
		Expect(err).ToNot(HaveOccurred())

		_, err = DeleteAgg(InventoryData{
			Product:     inventory,
			MongoTable:  mgTable,
			FilterName:  "Fruit_ID",
			FilterValue: inventory.FruitID,
		})
		Expect(err).To(HaveOccurred())
		// Expect(deleteResult.DeletedCount).To(Equal(int64(0)))
	})

	It("Should not delete without Fruit_ID", func() {
		inventory, err := GetInventoryJSON([]byte(mockdata.JsonForAddProduct()))
		Expect(err).ToNot(HaveOccurred())

		_, err = AddFood(InventoryData{
			Product:    inventory,
			MongoTable: mgTable,
		})
		Expect(err).ToNot(HaveOccurred())

		inventory, err = GetInventoryJSON([]byte(mockdata.JsonDelWithoutFruitID()))
		Expect(err).ToNot(HaveOccurred())

		_, err = DeleteAgg(InventoryData{
			Product:     inventory,
			MongoTable:  mgTable,
			FilterName:  "",
			FilterValue: inventory.FruitID,
		})
		Expect(err).To(HaveOccurred())
		// Expect(deleteResult.DeletedCount).To(Equal(int64(0)))
	})
})
