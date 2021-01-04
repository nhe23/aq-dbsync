package dataprocessor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"testing"

	"github.com/jarcoal/httpmock"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const url string = "https://api.openaq.org/v1/latest"

var dataAcc DataAccessInterface
var dataAccErr DataAccessInterface

func init() {
	dataAcc = NewDataAccess()
	dataAccErr = NewDataAccessError()
	httpmock.Activate()
	// defer httpmock.DeactivateAndReset()

	// Exact URL match
	httpmock.RegisterResponder("GET", url,
		httpmock.NewStringResponder(200, `{
			"meta": {
				"name": "openaq-api",
				"license": "CC BY 4.0",
				"website": "https://docs.openaq.org/",
				"page": 3,
				"limit": 1,
				"found": 12046
			},
			"results": [
				{
					"location": "1-r khoroolol",
					"city": "Ulaanbaatar",
					"country": "MN",
					"distance": 6563510.382773982,
					"measurements": [
						{
							"parameter": "pm10",
							"value": 199,
							"lastUpdated": "2019-03-13T21:45:00.000Z",
							"unit": "µg/m³",
							"sourceName": "Agaar.mn"
						},
						{
							"parameter": "pm25",
							"value": 217,
							"lastUpdated": "2019-03-13T21:45:00.000Z",
							"unit": "µg/m³",
							"sourceName": "Agaar.mn"
						},
						{
							"parameter": "so2",
							"value": 21,
							"lastUpdated": "2019-03-13T21:45:00.000Z",
							"unit": "µg/m³",
							"sourceName": "Agaar.mn"
						},
						{
							"parameter": "no2",
							"value": 30,
							"lastUpdated": "2019-03-13T21:45:00.000Z",
							"unit": "µg/m³",
							"sourceName": "Agaar.mn"
						},
						{
							"parameter": "co",
							"value": 57,
							"lastUpdated": "2019-03-13T21:45:00.000Z",
							"unit": "µg/m³",
							"sourceName": "Agaar.mn"
						}
					],
					"coordinates": { "latitude": 47.91798, "longitude": 106.84806 }
				}
			]
		}
		`))
}

func parseLocationResult(results []interface{}) locationResult {
	res, _ := json.Marshal(results[0])
	var loc locationResult
	json.Unmarshal([]byte(res), &loc)
	return loc
}

type dataAccess struct {
}

type dataAccessError struct {
}

// NewDataProcessor creates a dataProcessor.
func NewDataAccess() DataAccessInterface {
	return dataAccess{}
}

func (d dataAccess) BulkWrite(ctx context.Context, models []mongo.WriteModel, opts ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
	return nil, nil
}

// NewDataProcessor creates a dataProcessor.
func NewDataAccessError() DataAccessInterface {
	return dataAccessError{}
}

func (d dataAccessError) BulkWrite(ctx context.Context, models []mongo.WriteModel, opts ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
	return nil, fmt.Errorf("VERY BAD ERROR")
}
func Test_dataProcessor_GetResults(t *testing.T) {
	results := []interface{}{map[string]interface{}{"city": "Ulaanbaatar", "coordinates": map[string]interface{}{"latitude": 47.91798, "longitude": 106.84806}, "country": "MN", "distance": 6.563510382773982e+06, "location": "1-r khoroolol", "measurements": []interface{}{map[string]interface{}{"lastUpdated": "2019-03-13T21:45:00.000Z", "parameter": "pm10", "sourceName": "Agaar.mn", "unit": "µg/m³", "value": 199}, map[string]interface{}{"lastUpdated": "2019-03-13T21:45:00.000Z", "parameter": "pm25", "sourceName": "Agaar.mn", "unit": "µg/m³", "value": 217}, map[string]interface{}{"lastUpdated": "2019-03-13T21:45:00.000Z", "parameter": "so2", "sourceName": "Agaar.mn", "unit": "µg/m³", "value": 21}, map[string]interface{}{"lastUpdated": "2019-03-13T21:45:00.000Z", "parameter": "no2", "sourceName": "Agaar.mn", "unit": "µg/m³", "value": 30}, map[string]interface{}{"lastUpdated": "2019-03-13T21:45:00.000Z", "parameter": "co", "sourceName": "Agaar.mn", "unit": "µg/m³", "value": 57}}}}
	type fields struct {
		httpClient *http.Client
	}
	type args struct {
		url string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []interface{}
		want1  int
	}{
		{"standard", fields{http.DefaultClient}, args{url}, results, 12046},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := dataProcessor{
				httpClient: tt.fields.httpClient,
			}
			got, got1, err := d.getResults(tt.args.url)

			gotLoc := parseLocationResult(got)
			wantLoc := parseLocationResult(tt.want)
			if err != nil {
				t.Errorf("dataProcessor.GetResults() got error")
			}
			if !reflect.DeepEqual(gotLoc, wantLoc) {
				t.Errorf("dataProcessor.GetResults() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("dataProcessor.GetResults() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_dataProcessor_upsertCollection(t *testing.T) {
	results := []interface{}{map[string]interface{}{"city": "Ulaanbaatar", "coordinates": map[string]interface{}{"latitude": 47.91798, "longitude": 106.84806}, "country": "MN", "distance": 6.563510382773982e+06, "location": "1-r khoroolol", "measurements": []interface{}{map[string]interface{}{"lastUpdated": "2019-03-13T21:45:00.000Z", "parameter": "pm10", "sourceName": "Agaar.mn", "unit": "µg/m³", "value": 199}, map[string]interface{}{"lastUpdated": "2019-03-13T21:45:00.000Z", "parameter": "pm25", "sourceName": "Agaar.mn", "unit": "µg/m³", "value": 217}, map[string]interface{}{"lastUpdated": "2019-03-13T21:45:00.000Z", "parameter": "so2", "sourceName": "Agaar.mn", "unit": "µg/m³", "value": 21}, map[string]interface{}{"lastUpdated": "2019-03-13T21:45:00.000Z", "parameter": "no2", "sourceName": "Agaar.mn", "unit": "µg/m³", "value": 30}, map[string]interface{}{"lastUpdated": "2019-03-13T21:45:00.000Z", "parameter": "co", "sourceName": "Agaar.mn", "unit": "µg/m³", "value": 57}}}}
	type fields struct {
		httpClient *http.Client
	}
	type args struct {
		collection   DataAccessInterface
		resultsSlice []interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{"standard", fields{http.DefaultClient}, args{dataAcc, results}, false},
		{"error", fields{http.DefaultClient}, args{dataAccErr, results}, true},
	}
	var locResult locationResult
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := dataProcessor{
				httpClient: tt.fields.httpClient,
			}
			if err := d.upsertCollection(tt.args.collection, tt.args.resultsSlice, &locResult, &locResult.Location, "location"); (err != nil) != tt.wantErr {
				t.Errorf("upsertCollection() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_bulkUpdateResult(t *testing.T) {
	type args struct {
		collection DataAccessInterface
		operations []mongo.WriteModel
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"standard", args{dataAcc, nil}, false},
		{"error", args{dataAccErr, nil}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := bulkUpdateResult(tt.args.collection, tt.args.operations); (err != nil) != tt.wantErr {
				t.Errorf("bulkUpdateResult() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_deepCopy(t *testing.T) {
	type Test struct {
		Prop1 int
		Prop2 string
	}
	testObj := Test{1, "asdf"}
	type args struct {
		v interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    interface{}
		wantErr bool
	}{
		{"standard", args{testObj}, testObj, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := deepCopy(tt.args.v)
			if (err != nil) != tt.wantErr {
				t.Errorf("deepCopy() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("deepCopy() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_dataProcessor_ProcessData(t *testing.T) {
	mockDataProcessFunc := func(url string, collection DataAccessInterface) (int, error) {
		return 5, nil
	}
	mockDataProcessFuncError := func(url string, collection DataAccessInterface) (int, error) {
		return 0, fmt.Errorf("VERY BAD ERROR")
	}
	type fields struct {
		httpClient *http.Client
		batchSize  int
	}
	type args struct {
		url             string
		collection      DataAccessInterface
		dataProcessFunc func(url string, collection DataAccessInterface) (int, error)
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{"standard", fields{http.DefaultClient, 100}, args{"asdt", dataAcc, mockDataProcessFunc}, false},
		{"error", fields{http.DefaultClient, 100}, args{"asdt", dataAcc, mockDataProcessFuncError}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := dataProcessor{
				httpClient: tt.fields.httpClient,
				batchSize:  tt.fields.batchSize,
			}
			if err := d.ProcessData(tt.args.url, tt.args.collection, tt.args.dataProcessFunc); (err != nil) != tt.wantErr {
				t.Errorf("dataProcessor.ProcessData() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_dataProcessor_ProcessMeasurements(t *testing.T) {
	type fields struct {
		httpClient *http.Client
		batchSize  int
	}
	type args struct {
		url        string
		collection DataAccessInterface
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int
		wantErr bool
	}{
		{"standard", fields{http.DefaultClient, 100}, args{url, dataAcc}, 12046, false},
		{"error", fields{http.DefaultClient, 100}, args{url, dataAccErr}, 12046, true},
		{"errorURL", fields{http.DefaultClient, 100}, args{"nonexistanturl.test", dataAccErr}, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := dataProcessor{
				httpClient: tt.fields.httpClient,
				batchSize:  tt.fields.batchSize,
			}
			got, err := d.ProcessMeasurements(tt.args.url, tt.args.collection)
			if (err != nil) != tt.wantErr {
				t.Errorf("dataProcessor.ProcessMeasurements() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("dataProcessor.ProcessMeasurements() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_dataProcessor_ProcessCities(t *testing.T) {
	type fields struct {
		httpClient *http.Client
		batchSize  int
	}
	type args struct {
		url        string
		collection DataAccessInterface
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int
		wantErr bool
	}{
		{"standard", fields{http.DefaultClient, 100}, args{url, dataAcc}, 12046, false},
		{"error", fields{http.DefaultClient, 100}, args{url, dataAccErr}, 12046, true},
		{"errorURL", fields{http.DefaultClient, 100}, args{"nonexistanturl.test", dataAccErr}, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := dataProcessor{
				httpClient: tt.fields.httpClient,
				batchSize:  tt.fields.batchSize,
			}
			got, err := d.ProcessCities(tt.args.url, tt.args.collection)
			if (err != nil) != tt.wantErr {
				t.Errorf("dataProcessor.ProcessCities() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("dataProcessor.ProcessCities() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_dataProcessor_ProcessCountries(t *testing.T) {
	type fields struct {
		httpClient *http.Client
		batchSize  int
	}
	type args struct {
		url        string
		collection DataAccessInterface
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int
		wantErr bool
	}{
		{"standard", fields{http.DefaultClient, 100}, args{url, dataAcc}, 12046, false},
		{"error", fields{http.DefaultClient, 100}, args{url, dataAccErr}, 12046, true},
		{"errorURL", fields{http.DefaultClient, 100}, args{"nonexistanturl.test", dataAccErr}, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := dataProcessor{
				httpClient: tt.fields.httpClient,
				batchSize:  tt.fields.batchSize,
			}
			got, err := d.ProcessCountries(tt.args.url, tt.args.collection)
			if (err != nil) != tt.wantErr {
				t.Errorf("dataProcessor.ProcessCountries() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("dataProcessor.ProcessCountries() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_dataProcessor_upsertMeasurements(t *testing.T) {
	var results []locationResult
	resultsInterface := []interface{}{map[string]interface{}{"city": "Ulaanbaatar", "coordinates": map[string]interface{}{"latitude": 47.91798, "longitude": 106.84806}, "country": "MN", "distance": 6.563510382773982e+06, "location": "1-r khoroolol", "measurements": []interface{}{map[string]interface{}{"lastUpdated": "2019-03-13T21:45:00.000Z", "parameter": "pm10", "sourceName": "Agaar.mn", "unit": "µg/m³", "value": 199}, map[string]interface{}{"lastUpdated": "2019-03-13T21:45:00.000Z", "parameter": "pm25", "sourceName": "Agaar.mn", "unit": "µg/m³", "value": 217}, map[string]interface{}{"lastUpdated": "2019-03-13T21:45:00.000Z", "parameter": "so2", "sourceName": "Agaar.mn", "unit": "µg/m³", "value": 21}, map[string]interface{}{"lastUpdated": "2019-03-13T21:45:00.000Z", "parameter": "no2", "sourceName": "Agaar.mn", "unit": "µg/m³", "value": 30}, map[string]interface{}{"lastUpdated": "2019-03-13T21:45:00.000Z", "parameter": "co", "sourceName": "Agaar.mn", "unit": "µg/m³", "value": 57}}}}
	resultJSON, _ := json.Marshal(resultsInterface)
	json.Unmarshal([]byte(resultJSON), &results)
	type fields struct {
		httpClient *http.Client
		batchSize  int
	}
	type args struct {
		collection DataAccessInterface
		results    []locationResult
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{"standard", fields{http.DefaultClient, 10}, args{dataAcc, results}, false},
		{"error", fields{http.DefaultClient, 10}, args{dataAccErr, results}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := dataProcessor{
				httpClient: tt.fields.httpClient,
				batchSize:  tt.fields.batchSize,
			}
			if err := d.upsertMeasurements(tt.args.collection, tt.args.results); (err != nil) != tt.wantErr {
				t.Errorf("dataProcessor.upsertMeasurements() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
