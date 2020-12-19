package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/nhe23/aq-dbsync/pkg/dataprocessor"

	"github.com/jasonlvhit/gocron"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var ctx = context.TODO()

const measurementsColName = "measurements"
const citiesColName = "cities"
const countriesColName = "countries"

type dataProcessParams struct {
	url          string
	col          collection
	callBackFunc func(url string, collection *mongo.Collection) (int, error)
}

type collections struct {
	measurementCol collection
	citiesCol      collection
	countriesCol   collection
}

type collection struct {
	name string
	col  *mongo.Collection
}

var logger log.Logger

func initCollections(mongoURI string, dbName string) (collections, error) {
	var cols collections
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return cols, err
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		return cols, err
	}

	db := client.Database(dbName)
	cols.countriesCol.name = countriesColName
	cols.measurementCol.name = measurementsColName
	cols.citiesCol.name = citiesColName

	cols.countriesCol.col = db.Collection(cols.countriesCol.name)
	cols.citiesCol.col = db.Collection(cols.citiesCol.name)
	cols.measurementCol.col = db.Collection(cols.measurementCol.name)
	_, err = cols.measurementCol.col.Indexes().CreateOne(
		context.Background(),
		mongo.IndexModel{
			Keys: bson.M{
				"location": 1,
			},
			Options: options.Index().SetUnique(false),
		},
	)
	if err != nil {
		return cols, err
	}
	return cols, nil
}

func main() {
	var (
		fs             = flag.NewFlagSet("aqDataCrawler", flag.ExitOnError)
		aqAPI          = fs.String("aq-apiendpoint", "https://api.openaq.org", "The latest URL of the AQ api.")
		batchSize      = fs.Int("batch-size", 1000, "Number of results per request")
		httpRetryCount = fs.Int("http-retry-count", 3, "Number maximum retries of http requests")
		mongoURI       = fs.String("mongo-uri", "mongodb://localhost:27018", "Number of results per request")
		dbName         = fs.String("db-name", "AQ_DB", "Name of used mongo db")
		schedDuration  = fs.Uint64("scheduler-seconds", 3600, "Name of used mongo db")
	)
	logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	logger = level.NewFilter(logger, level.AllowInfo())
	logger = log.With(logger, "TS:", log.DefaultTimestamp, "caller", log.DefaultCaller)

	logger.Log("info", "Starting service")
	retryClient := retryablehttp.NewClient()
	retryClient.RetryMax = *httpRetryCount
	retryClient.RetryWaitMin = 5 * time.Second
	retryClient.Logger = logger.Log()
	httpClient := retryClient.StandardClient()
	cols, err := initCollections(*mongoURI, *dbName)
	if err != nil {
		logger.Log("err", "Error initializing mongo collections")
		logger.Log("err", err)
		os.Exit(1)
	}

	measurementsURL := fmt.Sprintf("%s/v1/latest?limit=%d&page=", *aqAPI, *batchSize)
	citiesURL := fmt.Sprintf("%s/v1/cities?limit=%d&page=", *aqAPI, *batchSize)
	countriesURL := fmt.Sprintf("%s/v1/countries?limit=%d&page=", *aqAPI, *batchSize)

	dataProcessor := dataprocessor.NewDataProcessor(httpClient, *batchSize)
	dataParams := make([]dataProcessParams, 0)
	dataParams = append(dataParams, dataProcessParams{citiesURL, cols.citiesCol, dataProcessor.ProcessCities})
	dataParams = append(dataParams, dataProcessParams{countriesURL, cols.countriesCol, dataProcessor.ProcessCountries})
	dataParams = append(dataParams, dataProcessParams{measurementsURL, cols.measurementCol, dataProcessor.ProcessMeasurements})
	for true {
		gocron.Every(*schedDuration).Seconds().From(gocron.NextTick()).Do(processAllData, dataProcessor, dataParams)
		<-gocron.Start()
	}
}

func processAllData(d dataprocessor.DataProcessor, dataParams []dataProcessParams) {
	for _, data := range dataParams {
		logger.Log("info", fmt.Sprintf("Processing data for %s", data.url))
		d.ProcessData(data.url, data.col.col, data.callBackFunc)
	}
}
