package integrations

import (
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"

	"github.com/influxdata/kapacitor"
	"github.com/influxdata/kapacitor/wlog"
	"github.com/influxdb/influxdb/client"
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

func compareResultsMetainfo(exp, got kapacitor.Result) (bool, string) {
	if (exp.Err == nil && got.Err != nil) || (exp.Err != nil && got.Err == nil) {
		return false, fmt.Sprintf("unexpected error: exp %v got %v", exp.Err, got.Err)
	}
	if exp.Err != nil && exp.Err.Error() != got.Err.Error() {
		return false, fmt.Sprintf("unexpected error: exp %v got %v", exp.Err, got.Err)
	}
	if len(exp.Series) != len(got.Series) {
		return false, fmt.Sprintf("unexpected number of series: exp %d got %d", len(exp.Series), len(got.Series))
	}
	return true, ""
}

func compareResults(exp, got kapacitor.Result) (bool, string) {
	ok, msg := compareResultsMetainfo(exp, got)
	if !ok {
		return ok, msg
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

func compareResultsIgnoreSeriesOrder(exp, got kapacitor.Result) (bool, string) {
	ok, msg := compareResultsMetainfo(exp, got)
	if !ok {
		return ok, msg
	}
	set := make(map[int]struct{}, len(exp.Series))
	for i := range exp.Series {
		set[i] = struct{}{}
	}
	for i := range exp.Series {
		// Find series with same name
		var j int
		for j = range set {
			if exp.Series[i].Name == got.Series[j].Name &&
				reflect.DeepEqual(exp.Series[i].Tags, got.Series[j].Tags) {
				// found matching series
				delete(set, j)
				break
			}
		}
		if !reflect.DeepEqual(exp.Series[i].Columns, got.Series[j].Columns) {
			return false, fmt.Sprintf("unexpected series columns: i: %d \nexp %v \ngot %v", i, exp.Series[i].Columns, got.Series[j].Columns)
		}
		if !reflect.DeepEqual(exp.Series[i].Values, got.Series[j].Values) {
			return false, fmt.Sprintf("unexpected series values: i: %d \nexp %v \ngot %v", i, exp.Series[i].Values, got.Series[j].Values)
		}
	}
	return true, ""
}

type LogService struct{}

func (l *LogService) NewLogger(prefix string, flag int) *log.Logger {
	return wlog.New(os.Stderr, prefix, flag)
}
