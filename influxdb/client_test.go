package influxdb

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestClient_Flux(t *testing.T) {
	t.Skip("Not Implemented")
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string
#group,false,false,true,true,false,false,true,true
,result,table,_start,_stop,_time,_value,_field,_measurement
,_result,0,2021-04-22T16:11:09.536898692Z,2021-04-22T16:13:09.536898692Z,2021-04-22T16:11:14.5346632Z,45,counter,boltdb_reads_total
,_result,0,2021-04-22T16:11:09.536898692Z,2021-04-22T16:13:09.536898692Z,2021-04-22T16:11:24.536408365Z,47,counter,boltdb_reads_total
,_result,0,2021-04-22T16:11:09.536898692Z,2021-04-22T16:13:09.536898692Z,2021-04-22T16:11:34.536392986Z,49,counter,boltdb_reads_total
,_result,1,2021-04-22T16:11:09.536898692Z,2021-04-22T16:13:09.536898692Z,2021-04-22T16:11:14.5346632Z,24,counter,boltdb_writes_total
,_result,1,2021-04-22T16:11:09.536898692Z,2021-04-22T16:13:09.536898692Z,2021-04-22T16:11:24.536408365Z,24,counter,boltdb_writes_total
,_result,1,2021-04-22T16:11:09.536898692Z,2021-04-22T16:13:09.536898692Z,2021-04-22T16:11:34.536392986Z,24,counter,boltdb_writes_total

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string
#group,false,false,true,true,false,false,true,true,true
,result,table,_start,_stop,_time,_value,_field,_measurement,version
,_result,10,2021-04-22T16:11:09.536898692Z,2021-04-22T16:13:09.536898692Z,2021-04-22T16:11:14.5346632Z,1,gauge,go_info,go1.15.2
,_result,10,2021-04-22T16:11:09.536898692Z,2021-04-22T16:13:09.536898692Z,2021-04-22T16:11:24.536408365Z,1,gauge,go_info,go1.15.2
,_result,10,2021-04-22T16:11:09.536898692Z,2021-04-22T16:13:09.536898692Z,2021-04-22T16:11:34.536392986Z,1,gauge,go_info,go1.15.2`))
	}))
	defer ts.Close()

	config := Config{
		URLs: []string{ts.URL},
		Credentials: Credentials{
			Token:  "faketoken",
			Method: TokenAuthentication,
		},
	}
	c, err := NewHTTPClient(config)
	if err != nil {
		t.Fatal(err)
	}
	q := FluxQuery{Query: `from(bucket:"testbucket")|>range(start:-2m )`, Org: "testorg"}
	_, err = c.QueryFlux(q)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
}

func TestClient_Query(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data Response
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(data)
	}))
	defer ts.Close()

	config := Config{URLs: []string{ts.URL}}
	c, _ := NewHTTPClient(config)

	query := Query{}
	_, err := c.Query(query)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
}

func TestClient_BasicAuth(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()

		if !ok {
			t.Errorf("basic auth error")
		}
		if u != "username" {
			t.Errorf("unexpected username, expected %q, actual %q", "username", u)
		}
		if p != "password" {
			t.Errorf("unexpected password, expected %q, actual %q", "password", p)
		}
		var data Response
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(data)
	}))
	defer ts.Close()

	config := Config{URLs: []string{ts.URL}, Credentials: Credentials{Method: UserAuthentication, Username: "username", Password: "password"}}
	c, _ := NewHTTPClient(config)

	query := Query{}
	_, err := c.Query(query)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
}

func TestClient_Ping(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data Response
		w.WriteHeader(http.StatusNoContent)
		_ = json.NewEncoder(w).Encode(data)
	}))
	defer ts.Close()

	config := Config{URLs: []string{ts.URL}}
	c, _ := NewHTTPClient(config)

	_, _, err := c.Ping(nil)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
}

func TestClient_Update(t *testing.T) {
	ts0 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data Response
		w.WriteHeader(http.StatusNoContent)
		_ = json.NewEncoder(w).Encode(data)
	}))
	defer ts0.Close()

	config := Config{URLs: []string{ts0.URL}}
	c, _ := NewHTTPClient(config)

	_, _, err := c.Ping(nil)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}

	ts1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data Response
		w.WriteHeader(http.StatusNoContent)
		_ = json.NewEncoder(w).Encode(data)
	}))
	defer ts1.Close()
	config.URLs = []string{ts1.URL}
	c.Update(config)

	_, _, err = c.Ping(nil)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
}

func TestClient_Concurrent_Use(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	}))
	defer ts.Close()

	config := Config{URLs: []string{ts.URL}}
	c, _ := NewHTTPClient(config)

	var wg sync.WaitGroup
	wg.Add(3)
	n := 1000

	go func() {
		defer wg.Done()
		bp, err := NewBatchPoints(BatchPointsConfig{})
		if err != nil {
			t.Errorf("got error %v", err)
		}

		for i := 0; i < n; i++ {
			if err = c.Write(bp); err != nil {
				t.Errorf("got error %v", err)
			}
		}
	}()

	go func() {
		defer wg.Done()
		var q Query
		for i := 0; i < n; i++ {
			if _, err := c.Query(q); err != nil {
				t.Errorf("got error %v", err)
			}
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			c.Ping(nil)
		}
	}()
	wg.Wait()
}

func TestClient_Write(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data Response
		uncompressedBody, err := gzip.NewReader(r.Body)
		if err != nil {
			t.Error(err)
		}
		bod, err := ioutil.ReadAll(uncompressedBody)
		if err != nil {
			t.Error(err)
		}
		if r.Header.Get("Content-Encoding") != "gzip" {
			t.Errorf("expected gzip Content-Encoding but got %s", r.Header.Get("Content-Encoding"))
		}
		expected := "testpt,tag1=tag1 value=1i 942105600000000003\n"
		if string(bod) != expected {
			t.Errorf("unexpected send, expected '%s', got '%s'", expected, string(bod))
		}
		w.WriteHeader(http.StatusNoContent)
		_ = json.NewEncoder(w).Encode(data)
	}))
	defer ts.Close()

	config := Config{URLs: []string{ts.URL}}
	c, _ := NewHTTPClient(config)

	bp, err := NewBatchPoints(BatchPointsConfig{})
	bp.AddPoint(Point{
		Name:   "testpt",
		Tags:   map[string]string{"tag1": "tag1"},
		Fields: map[string]interface{}{"value": 1},
		Time:   time.Date(1999, 11, 9, 0, 0, 0, 3, time.UTC),
	})
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
	err = c.Write(bp)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
}

func TestClient_WriteLarge(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping slow test that writes a batch of 100,000 points")
	}
	expected := &bytes.Buffer{}
	wg := sync.WaitGroup{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data Response
		uncompressedBody, err := gzip.NewReader(r.Body)
		if err != nil {
			t.Error(err)
		}
		bod, err := ioutil.ReadAll(uncompressedBody)
		if err != nil {
			t.Error(err)
		}
		if r.Header.Get("Content-Encoding") != "gzip" {
			t.Errorf("expected gzip Content-Encoding but got %s", r.Header.Get("Content-Encoding"))
		}
		if string(bod) != expected.String() {
			t.Errorf("unexpected send:\n%s",
				cmp.Diff(
					strings.Split(expected.String(), "\n"),
					strings.Split(string(bod), "\n")))
		}
		w.WriteHeader(http.StatusNoContent)
		_ = json.NewEncoder(w).Encode(data)
		wg.Done()
	}))
	defer ts.Close()

	config := Config{URLs: []string{ts.URL}}
	c, _ := NewHTTPClient(config)

	bp, err := NewBatchPoints(BatchPointsConfig{})
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	startT := time.Date(1999, 11, 9, 0, 0, 0, 3, time.UTC)
	for i := 0; i < 100000; i++ {
		pt := Point{
			Name:   "testpt",
			Tags:   map[string]string{"tag1": "tag1"},
			Fields: map[string]interface{}{"value": 1},
			Time:   startT.Add(time.Second * time.Duration(i)),
		}
		bp.AddPoint(pt)
		expected.Write(pt.BytesWithLineFeed(bp.Precision()))
	}

	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
	wg.Add(1)
	err = c.Write(bp)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
	wg.Wait()
}

func TestClient_Write_noCompression(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data Response
		bod, err := ioutil.ReadAll(r.Body)
		expected := "testpt,tag1=tag1 value=1i 942105600000000003\n"
		if string(bod) != expected {
			t.Errorf("unexpected send, expected '%s', got '%s'", expected, string(bod))
		}
		if err != nil {
			t.Error(err)
		}
		w.WriteHeader(http.StatusNoContent)
		_ = json.NewEncoder(w).Encode(data)
	}))
	defer ts.Close()

	config := Config{
		URLs:        []string{ts.URL},
		Compression: "none",
	}
	c, _ := NewHTTPClient(config)

	bp, err := NewBatchPoints(BatchPointsConfig{})
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}

	bp.AddPoint(Point{
		Name:   "testpt",
		Tags:   map[string]string{"tag1": "tag1"},
		Fields: map[string]interface{}{"value": 1},
		Time:   time.Date(1999, 11, 9, 0, 0, 0, 3, time.UTC),
	})
	err = c.Write(bp)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
}

func TestClient_UserAgent(t *testing.T) {
	receivedUserAgent := ""
	var code int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedUserAgent = r.UserAgent()

		var data Response
		w.WriteHeader(code)
		json.NewEncoder(w).Encode(data)
	}))
	defer ts.Close()

	tests := []struct {
		name      string
		userAgent string
		expected  string
	}{
		{
			name:      "Empty user agent",
			userAgent: "",
			expected:  "KapacitorInfluxDBClient",
		},
		{
			name:      "Custom user agent",
			userAgent: "Test Influx Client",
			expected:  "Test Influx Client",
		},
	}

	for _, test := range tests {
		var err error

		config := Config{URLs: []string{ts.URL}, UserAgent: test.userAgent}
		c, _ := NewHTTPClient(config)

		receivedUserAgent = ""
		code = http.StatusOK
		query := Query{}
		_, err = c.Query(query)
		if err != nil {
			t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
		}
		if !strings.HasPrefix(receivedUserAgent, test.expected) {
			t.Errorf("Unexpected user agent for query request. expected %v, actual %v", test.expected, receivedUserAgent)
		}

		receivedUserAgent = ""
		code = http.StatusNoContent
		bp, _ := NewBatchPoints(BatchPointsConfig{})
		err = c.Write(bp)
		if err != nil {
			t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
		}
		if !strings.HasPrefix(receivedUserAgent, test.expected) {
			t.Errorf("Unexpected user agent for write request. expected %v, actual %v", test.expected, receivedUserAgent)
		}

		receivedUserAgent = ""
		code = http.StatusNoContent
		_, _, err = c.Ping(nil)
		if err != nil {
			t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
		}
		if receivedUserAgent != test.expected {
			t.Errorf("Unexpected user agent for ping request. expected %v, actual %v", test.expected, receivedUserAgent)
		}
	}
}

func TestBatchPoints_SettersGetters(t *testing.T) {
	bp, _ := NewBatchPoints(BatchPointsConfig{
		Precision:        "ns",
		Database:         "db",
		RetentionPolicy:  "rp",
		WriteConsistency: "wc",
	})
	if bp.Precision() != "ns" {
		t.Errorf("Expected: %s, got %s", bp.Precision(), "ns")
	}
	if bp.Database() != "db" {
		t.Errorf("Expected: %s, got %s", bp.Database(), "db")
	}
	if bp.RetentionPolicy() != "rp" {
		t.Errorf("Expected: %s, got %s", bp.RetentionPolicy(), "rp")
	}
	if bp.WriteConsistency() != "wc" {
		t.Errorf("Expected: %s, got %s", bp.WriteConsistency(), "wc")
	}

	bp.SetDatabase("db2")
	bp.SetRetentionPolicy("rp2")
	bp.SetWriteConsistency("wc2")
	err := bp.SetPrecision("s")
	if err != nil {
		t.Errorf("Did not expect error: %s", err.Error())
	}

	if bp.Precision() != "s" {
		t.Errorf("Expected: %s, got %s", bp.Precision(), "s")
	}
	if bp.Database() != "db2" {
		t.Errorf("Expected: %s, got %s", bp.Database(), "db2")
	}
	if bp.RetentionPolicy() != "rp2" {
		t.Errorf("Expected: %s, got %s", bp.RetentionPolicy(), "rp2")
	}
	if bp.WriteConsistency() != "wc2" {
		t.Errorf("Expected: %s, got %s", bp.WriteConsistency(), "wc2")
	}
}
