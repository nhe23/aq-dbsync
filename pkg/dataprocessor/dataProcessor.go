package dataprocessor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"reflect"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type cityResult struct {
	Name      string `bson:"name"`
	Country   string `bson:"country"`
	Count     int    `bson:"count"`
	Locations int    `bson:"locations"`
}

type countryResult struct {
	Code      string `bson:"code"`
	Name      string `bson:"name"`
	Count     int    `bson:"count"`
	Cities    int    `bson:"cities"`
	Locations int    `bson:"locations"`
}

type locationResult struct {
	Location     string        `bson:"location"`
	City         string        `bson:"city"`
	Country      string        `bson:"country"`
	Measurements []measurement `bson:"measurements"`
	Coordinates  coordinates   `bson:"coordinates"`
}

type measurement struct {
	Parameter   string    `bson:"parameter"`
	Value       int       `bson:"value"`
	LastUpdated time.Time `bson:"updated_at"`
	Unit        string    `bson:"unit"`
}

type coordinates struct {
	Latitude  float64 `bson:"latitude"`
	Longitude float64 `bson:"longitude"`
}

type dataProcessor struct {
	httpClient *http.Client
	batchSize  int
}

type DataAccessInterface interface {
	BulkWrite(ctx context.Context, models []mongo.WriteModel, opts ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error)
}

//
type DataProcessor interface {
	// GetMeasurementResults(url string) ([]interface{}, int)
	// UpsertMeasurements(colletion *mongo.Collection, resultsSlice []interface{})
	ProcessMeasurements(url string, colletion *mongo.Collection) int
	ProcessCities(url string, collection *mongo.Collection) int
	ProcessCountries(url string, collection *mongo.Collection) int
	ProcessData(url string, collection *mongo.Collection, dataProcessFunc func(url string, collection *mongo.Collection) int)
}

// NewDataProcessor creates a dataProcessor.
func NewDataProcessor(httpClient *http.Client, batchSize int) DataProcessor {
	return dataProcessor{httpClient, batchSize}
}

func (d dataProcessor) ProcessMeasurements(url string, collection *mongo.Collection) int {
	resultsSlice, total := d.getResults(url)
	var locResult locationResult
	d.upsertCollection(collection, resultsSlice, &locResult, &locResult.Location, "location")
	return total
}

func (d dataProcessor) ProcessCities(url string, collection *mongo.Collection) int {
	resultsSlice, total := d.getResults(url)
	var cityRes cityResult
	d.upsertCollection(collection, resultsSlice, &cityRes, &cityRes.Name, "name")
	return total
}

func (d dataProcessor) ProcessCountries(url string, collection *mongo.Collection) int {
	resultsSlice, total := d.getResults(url)
	var countryRes countryResult
	d.upsertCollection(collection, resultsSlice, &countryRes, &countryRes.Code, "code")
	return total
}

func (d dataProcessor) ProcessData(url string, collection *mongo.Collection, dataProcessFunc func(url string, collection *mongo.Collection) int) {
	page := 1
	total := dataProcessFunc(fmt.Sprintf("%s%d", url, page), collection)
	for i := d.batchSize; i <= total; i += d.batchSize {
		page++
		dataProcessFunc(fmt.Sprintf("%s%d", url, page), collection)
	}
}

func (d dataProcessor) getResults(url string) ([]interface{}, int) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Fatal("failed to creating a request: %w", err)
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		log.Fatal((err))
	}

	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	results, exists := result["results"]
	if !exists {
		log.Fatal("no results")
	}

	resultsArray, ok := results.([]interface{})
	if !ok {
		log.Fatal("results is not of type ")
	}

	meta, exists := result["meta"]
	if !exists {
		log.Fatal("no meta data")
	}
	metaMap, ok := meta.(map[string]interface{})
	if !ok {
		log.Fatal("results is not of type ")
	}
	total, exists := metaMap["found"].(float64)
	if !exists {
		fmt.Println("No valid meta data found")
	}
	return resultsArray, int(total)
}

func (d dataProcessor) upsertCollection(collection DataAccessInterface, resultsSlice []interface{}, r interface{}, filter *string, filterName string) {
	var operations []mongo.WriteModel

	for _, result := range resultsSlice {
		resultJSON, err := json.Marshal(result)
		if err != nil {
			log.Fatal(err)
		}
		json.Unmarshal([]byte(resultJSON), &r)

		mongoOperation := mongo.NewReplaceOneModel()
		fmt.Printf("%#v \n", *filter)
		resultCopy, err := deepCopy(r)
		if err != nil {
			log.Fatal(err)
		}
		mongoOperation.SetFilter(bson.M{"name": *filter})
		mongoOperation.SetReplacement(resultCopy)
		mongoOperation.SetUpsert(true)
		operations = append(operations, mongoOperation)
	}
	err := bulkUpdateResult(collection, operations)
	if err != nil {
		log.Fatal(err)
	}
}

func bulkUpdateResult(collection DataAccessInterface, operations []mongo.WriteModel) error {
	// Specify an option to turn the bulk insertion in order of operation
	bulkOption := options.BulkWriteOptions{}
	bulkOption.SetOrdered(true)

	_, err := collection.BulkWrite(context.Background(), operations, &bulkOption)
	if err != nil {
		return err
	}
	return nil
}

func deepCopy(v interface{}) (interface{}, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}

	vptr := reflect.New(reflect.TypeOf(v))
	err = json.Unmarshal(data, vptr.Interface())
	if err != nil {
		return nil, err
	}
	return vptr.Elem().Interface(), err
}
