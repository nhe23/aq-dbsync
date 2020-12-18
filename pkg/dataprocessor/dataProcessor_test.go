package dataprocessor

import (
	"encoding/json"
	"net/http"
	"reflect"
	"testing"

	"github.com/jarcoal/httpmock"
	"go.mongodb.org/mongo-driver/mongo"
)

func parseLocationResult(results []interface{}) locationResult {
	res, _ := json.Marshal(results[0])
	var loc locationResult
	json.Unmarshal([]byte(res), &loc)
	return loc
}
func Test_dataProcessor_GetResults(t *testing.T) {
	url := "https://api.openaq.org/v1/latest"
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

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
			got, got1 := d.getResults(tt.args.url)

			gotLoc := parseLocationResult(got)
			wantLoc := parseLocationResult(tt.want)
			if !reflect.DeepEqual(gotLoc, wantLoc) {
				t.Errorf("dataProcessor.GetResults() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("dataProcessor.GetResults() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_dataProcessor_upsertMeasurements(t *testing.T) {
	type fields struct {
		httpClient *http.Client
	}
	type args struct {
		collection   *mongo.Collection
		resultsSlice []interface{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := dataProcessor{
				httpClient: tt.fields.httpClient,
			}
			d.upsertMeasurements(tt.args.collection, tt.args.resultsSlice)
		})
	}
}
