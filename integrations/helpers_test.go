package integrations

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"

	"github.com/influxdb/influxdb/client"
	"github.com/influxdb/kapacitor"
)

type MockInfluxDBService struct {
	ts *httptest.Server
}

func NewMockInfluxDBService(h http.Handler) *MockInfluxDBService {
	ts := httptest.NewServer(h)
	return &MockInfluxDBService{
		ts: ts,
	}
}

func (m *MockInfluxDBService) NewClient() (*client.Client, error) {
	u, _ := url.Parse(m.ts.URL)
	return client.NewClient(client.Config{
		URL:       *u,
		Precision: "s",
	})

}

func compareResults(exp, got kapacitor.Result) (bool, string) {
	if (exp.Err == nil && got.Err != nil) || (exp.Err != nil && got.Err == nil) {
		return false, fmt.Sprintf("unexpected error: exp %p got %p", exp.Err, got.Err)
	}
	if exp.Err != nil && exp.Err.Error() != got.Err.Error() {
		return false, fmt.Sprintf("unexpected error: exp %v got %v", exp.Err, got.Err)
	}
	if len(exp.Series) != len(got.Series) {
		return false, fmt.Sprintf("unexpected number of series: exp %d got %d", len(exp.Series), len(got.Series))
	}
	for i := range exp.Series {
		if exp.Series[i].Name != got.Series[i].Name {
			return false, fmt.Sprintf("unexpected series name: i: %d exp %s got %s", i, exp.Series[i].Name, got.Series[i].Name)
		}
		if !reflect.DeepEqual(exp.Series[i].Tags, got.Series[i].Tags) {
			return false, fmt.Sprintf("unexpected series tags: i: %d \nexp %v \ngot %v", i, exp.Series[i].Tags, got.Series[i].Tags)
		}
		if !reflect.DeepEqual(exp.Series[i].Columns, got.Series[i].Columns) {
			return false, fmt.Sprintf("unexpected series columns: i: %d \nexp %v \ngot %v", i, exp.Series[i].Columns, got.Series[i].Columns)
		}
		if !reflect.DeepEqual(exp.Series[i].Values, got.Series[i].Values) {
			return false, fmt.Sprintf("unexpected series values: i: %d \nexp %v \ngot %v", i, exp.Series[i].Values, got.Series[i].Values)
		}
	}
	return true, ""
}
