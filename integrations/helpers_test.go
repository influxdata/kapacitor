package integrations

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"reflect"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/kapacitor"
	"github.com/influxdata/kapacitor/influxdb"
	k8s "github.com/influxdata/kapacitor/services/k8s/client"
	"github.com/influxdata/kapacitor/udf"
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

func (m *MockInfluxDBService) NewNamedClient(name string) (influxdb.Client, error) {
	return influxdb.NewHTTPClient(influxdb.Config{
		URLs: []string{m.ts.URL},
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
		found := false
		for j = range set {
			if exp.Series[i].Name == got.Series[j].Name &&
				reflect.DeepEqual(exp.Series[i].Tags, got.Series[j].Tags) {
				// found matching series
				delete(set, j)
				found = true
				break
			}
		}
		if !found {
			return false, fmt.Sprintf("could not find matching series: %s %v", exp.Series[i].Name, exp.Series[i].Tags)
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

func compareAlertData(exp, got kapacitor.AlertData) (bool, string) {
	// Pull out Result for comparison
	expData := kapacitor.Result(exp.Data)
	exp.Data = influxql.Result{}
	gotData := kapacitor.Result(got.Data)
	kapacitor.ConvertResultTimes(&gotData)
	got.Data = influxql.Result{}

	if !reflect.DeepEqual(got, exp) {
		return false, fmt.Sprintf("\ngot %v\nexp %v", got, exp)
	}

	return compareResults(expData, gotData)
}

type UDFService struct {
	ListFunc   func() []string
	InfoFunc   func(name string) (udf.Info, bool)
	CreateFunc func(name string, l *log.Logger, abortCallback func()) (udf.Interface, error)
}

func (u UDFService) List() []string {
	return u.ListFunc()
}

func (u UDFService) Info(name string) (udf.Info, bool) {
	return u.InfoFunc(name)
}

func (u UDFService) Create(name string, l *log.Logger, abortCallback func()) (udf.Interface, error) {
	return u.CreateFunc(name, l, abortCallback)
}

type taskStore struct{}

func (ts taskStore) SaveSnapshot(name string, snapshot *kapacitor.TaskSnapshot) error { return nil }
func (ts taskStore) HasSnapshot(name string) bool                                     { return false }
func (ts taskStore) LoadSnapshot(name string) (*kapacitor.TaskSnapshot, error) {
	return nil, errors.New("not implemented")
}

type deadman struct {
	interval  time.Duration
	threshold float64
	id        string
	message   string
	global    bool
}

func (d deadman) Interval() time.Duration { return d.interval }
func (d deadman) Threshold() float64      { return d.threshold }
func (d deadman) Id() string              { return d.id }
func (d deadman) Message() string         { return d.message }
func (d deadman) Global() bool            { return d.global }

type k8sAutoscale struct {
	ScalesGetFunc    func(kind, name string) (*k8s.Scale, error)
	ScalesUpdateFunc func(kind string, scale *k8s.Scale) error
}
type k8sScales struct {
	ScalesGetFunc    func(kind, name string) (*k8s.Scale, error)
	ScalesUpdateFunc func(kind string, scale *k8s.Scale) error
}

func (k k8sAutoscale) Client() (k8s.Client, error) {
	return k, nil
}
func (k k8sAutoscale) Scales(namespace string) k8s.ScalesInterface {
	return k8sScales{
		ScalesGetFunc:    k.ScalesGetFunc,
		ScalesUpdateFunc: k.ScalesUpdateFunc,
	}
}
func (k k8sAutoscale) Update(c k8s.Config) error {
	return nil
}

func (k k8sScales) Get(kind, name string) (*k8s.Scale, error) {
	return k.ScalesGetFunc(kind, name)
}
func (k k8sScales) Update(kind string, scale *k8s.Scale) error {
	return k.ScalesUpdateFunc(kind, scale)
}
