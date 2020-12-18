package main

import (
	"testing"
)

func Test_main(t *testing.T) {
	tests := []struct {
		name string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			main()
		})
	}
}

func TestGetURL(t *testing.T) {
	type args struct {
		formatURL string
		batchSize int
		page      int
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{"AQ_URL", args{"https://api.openaq.org/v1/latest?limit=%d&page=%d", 1, 1}, "https://api.openaq.org/v1/latest?limit=1&page=1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetURL(tt.args.formatURL, tt.args.batchSize, tt.args.page); got != tt.want {
				t.Errorf("GetURL() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestProcessResultData(t *testing.T) {
	type args struct {
		url string
	}
	tests := []struct {
		name    string
		args    args
		wantMin int
	}{
		{"standardURL", args{"https://api.openaq.org/v1/latest?limit=1&page=1"}, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ProcessResultData(tt.args.url); got < tt.wantMin {
				t.Errorf("ProcessResultData() = %v, is smaller than %v", got, tt.wantMin)
			}
		})
	}
}
