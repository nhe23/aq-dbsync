package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/nhe23/aq-dbsync/pkg/dataprocessor"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var ctx = context.TODO()

const measurementsColName = "measurements"
const citiesColName = "cities"
const countriesColName = "countries"

type collections struct {
	measurementCol collection
	citiesCol      collection
	countriesCol   collection
}

type collection struct {
	name       string
	collection *mongo.Collection
}

func initCollections(mongoURI string, dbName string) collections {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatal(err)
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}

	db := client.Database(dbName)
	var cols collections

	cols.countriesCol.name = countriesColName
	cols.measurementCol.name = measurementsColName
	cols.citiesCol.name = citiesColName

	cols.countriesCol.collection = db.Collection(cols.countriesCol.name)
	cols.citiesCol.collection = db.Collection(cols.citiesCol.name)
	cols.measurementCol.collection = db.Collection(cols.measurementCol.name)
	indexName, err := cols.measurementCol.collection.Indexes().CreateOne(
		context.Background(),
		mongo.IndexModel{
			Keys: bson.M{
				"location": 1,
			},
			Options: options.Index().SetUnique(false),
		},
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(indexName)
	return cols
}

func main() {
	var (
		fs             = flag.NewFlagSet("aqDataCrawler", flag.ExitOnError)
		aqAPI          = fs.String("aq-apiendpoint", "https://api.openaq.org", "The latest URL of the AQ api.")
		batchSize      = fs.Int("batch-size", 1000, "Number of results per request")
		httpRetryCount = fs.Int("http-retry-count", 3, "Number maximum retries of http requests")
		mongoURI       = fs.String("mongo-uri", "mongodb://localhost:27018", "Number of results per request")
		dbName         = fs.String("db-name", "AQ_DB", "Name of used mongo db")
	)
	fmt.Println("Starting")
	retryClient := retryablehttp.NewClient()
	retryClient.RetryMax = *httpRetryCount
	retryClient.RetryWaitMin = 5 * time.Second
	httpClient := retryClient.StandardClient()
	cols := initCollections(*mongoURI, *dbName)

	// measurementsURL := fmt.Sprintf("%s/v1/latest?limit=%d&page=", *aqAPI, *batchSize)
	citiesURL := fmt.Sprintf("%s/v1/cities?limit=%d&page=", *aqAPI, *batchSize)
	// countriesURL := fmt.Sprintf("%s/v1/countries?limit=%d&page=", *aqAPI, *batchSize)

	dataProcessor := dataprocessor.NewDataProcessor(httpClient, *batchSize)
	dataProcessor.ProcessData(citiesURL, cols.citiesCol.collection, dataProcessor.ProcessCities)
	// dataProcessor.ProcessData(countriesURL, cols.countriesCol.collection, dataProcessor.ProcessCountries)
	// dataProcessor.ProcessData(measurementsURL, cols.measurementCol.collection, dataProcessor.ProcessMeasurements)
	// total := dataProcessor.ProcessMeasurements(fmt.Sprintf("%s%d", measurementsURL, page), cols.measurementCol.collection)
	// for i := *batchSize; i <= total; i += *batchSize {
	// 	page++
	// 	dataProcessor.ProcessMeasurements(fmt.Sprintf("%s%d", measurementsURL, page), cols.measurementCol.collection)
	// }
}
