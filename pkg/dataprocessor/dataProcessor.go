package dataprocessor

import (
	"context"
	"encoding/json"
	"fmt"
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

// DataAccessInterface that consists of all used mongo function.
type DataAccessInterface interface {
	BulkWrite(ctx context.Context, models []mongo.WriteModel, opts ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error)
}

// DataProcessor interface for methods
type DataProcessor interface {
	ProcessMeasurements(url string, colletion DataAccessInterface) (int, error)
	ProcessCities(url string, collection DataAccessInterface) (int, error)
	ProcessCountries(url string, collection DataAccessInterface) (int, error)
	ProcessData(url string, collection DataAccessInterface, dataProcessFunc func(url string, collection DataAccessInterface) (int, error)) error
}

// NewDataProcessor creates a dataProcessor.
func NewDataProcessor(httpClient *http.Client, batchSize int) DataProcessor {
	return dataProcessor{httpClient, batchSize}
}

func (d dataProcessor) ProcessMeasurements(url string, collection DataAccessInterface) (int, error) {
	resultsSlice, total, err := d.getResults(url)
	if err != nil {
		return 0, err
	}
	var locResult locationResult
	err = d.upsertCollection(collection, resultsSlice, &locResult, &locResult.Location, "location")
	return total, err
}

func (d dataProcessor) ProcessCities(url string, collection DataAccessInterface) (int, error) {
	resultsSlice, total, err := d.getResults(url)
	if err != nil {
		return 0, err
	}
	var cityRes cityResult
	err = d.upsertCollection(collection, resultsSlice, &cityRes, &cityRes.Name, "name")
	return total, err
}

func (d dataProcessor) ProcessCountries(url string, collection DataAccessInterface) (int, error) {
	resultsSlice, total, err := d.getResults(url)
	if err != nil {
		return 0, err
	}
	var countryRes countryResult
	err = d.upsertCollection(collection, resultsSlice, &countryRes, &countryRes.Code, "code")
	return total, err
}

func (d dataProcessor) ProcessData(url string, collection DataAccessInterface, dataProcessFunc func(url string, collection DataAccessInterface) (int, error)) error {
	page := 1
	total, err := dataProcessFunc(fmt.Sprintf("%s%d", url, page), collection)
	if err != nil {
		return fmt.Errorf("error processing data for url %s: %w", url, err)
	}
	for i := d.batchSize; i <= total; i += d.batchSize {
		page++
		dataProcessFunc(fmt.Sprintf("%s%d", url, page), collection)
	}
	return nil
}

func (d dataProcessor) getResults(url string) ([]interface{}, int, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to creating a request: %w", err)
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to send request: %w", err)
	}

	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	results, exists := result["results"]
	if !exists {
		return nil, 0, fmt.Errorf("no results object present")
	}

	resultsArray, ok := results.([]interface{})
	if !ok {
		return nil, 0, fmt.Errorf("could not pars results array")
	}

	meta, exists := result["meta"]
	if !exists {
		return resultsArray, 0, fmt.Errorf("no meta data available")
	}
	metaMap, ok := meta.(map[string]interface{})
	if !ok {
		return resultsArray, 0, fmt.Errorf("could not pars meta object")
	}
	total, exists := metaMap["found"].(float64)
	if !exists {
		return resultsArray, 0, fmt.Errorf("No valid meta data found")
	}
	return resultsArray, int(total), nil
}

func (d dataProcessor) upsertCollection(
	collection DataAccessInterface,
	resultsSlice []interface{},
	r interface{},
	filter *string,
	filterName string) error {
	var operations []mongo.WriteModel

	for _, result := range resultsSlice {
		resultJSON, err := json.Marshal(result)
		if err != nil {
			return fmt.Errorf("error converting json: %w", err)
		}
		json.Unmarshal([]byte(resultJSON), &r)

		mongoOperation := mongo.NewReplaceOneModel()
		resultCopy, err := deepCopy(r)
		if err != nil {
			return fmt.Errorf("error copying json: %w", err)
		}
		mongoOperation.SetFilter(bson.M{filterName: *filter})
		mongoOperation.SetReplacement(resultCopy)
		mongoOperation.SetUpsert(true)
		operations = append(operations, mongoOperation)
	}
	err := bulkUpdateResult(collection, operations)
	if err != nil {
		return fmt.Errorf("error updating collection: %w", err)
	}
	return nil
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
