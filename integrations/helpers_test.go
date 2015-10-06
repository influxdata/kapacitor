package integrations

import (
	"net/http"
	"net/http/httptest"
	"net/url"

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
