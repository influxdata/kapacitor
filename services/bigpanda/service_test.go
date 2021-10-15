package bigpanda

import (
	"bytes"
	"testing"
	"time"

	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/models"
)

func TestService_SerializeEventData(t *testing.T) {
	config := Config{Enabled: true, AppKey: "key", AutoAttributes: "tags,fields"}
	s, err := NewService(config, nil)
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		name    string
		tags    map[string]string
		fields  map[string]interface{}
		attrs   map[string]string
		expBody string
	}{
		{
			name:    "int field",
			fields:  map[string]interface{}{"primitive_type": 10},
			expBody: "{\"app_key\":\"key\",\"check\":\"id\",\"description\":\"message\",\"details\":\"details\",\"primitive_type\":\"10\",\"status\":\"ok\",\"task\":\":test\",\"timestamp\":31536038}",
		},
		{
			name:    "string field",
			fields:  map[string]interface{}{"primitive_type": "string"},
			expBody: "{\"app_key\":\"key\",\"check\":\"id\",\"description\":\"message\",\"details\":\"details\",\"primitive_type\":\"string\",\"status\":\"ok\",\"task\":\":test\",\"timestamp\":31536038}",
		},
		{
			name:    "boolean field",
			fields:  map[string]interface{}{"primitive_type": true},
			expBody: "{\"app_key\":\"key\",\"check\":\"id\",\"description\":\"message\",\"details\":\"details\",\"primitive_type\":\"true\",\"status\":\"ok\",\"task\":\":test\",\"timestamp\":31536038}",
		},
		{
			name:    "float field",
			fields:  map[string]interface{}{"primitive_type": 123.45},
			expBody: "{\"app_key\":\"key\",\"check\":\"id\",\"description\":\"message\",\"details\":\"details\",\"primitive_type\":\"123.45\",\"status\":\"ok\",\"task\":\":test\",\"timestamp\":31536038}",
		},
		{
			name:    "html field",
			fields:  map[string]interface{}{"escape": "\n"},
			expBody: "{\"app_key\":\"key\",\"check\":\"id\",\"description\":\"message\",\"details\":\"details\",\"escape\":\"\\n\",\"status\":\"ok\",\"task\":\":test\",\"timestamp\":31536038}",
		},
		{
			name:    "array field",
			fields:  map[string]interface{}{"array": []interface{}{10, true, "string value"}},
			expBody: "{\"app_key\":\"key\",\"array\":\"[10,true,\\\"string value\\\"]\",\"check\":\"id\",\"description\":\"message\",\"details\":\"details\",\"status\":\"ok\",\"task\":\":test\",\"timestamp\":31536038}",
		},
		{
			name:    "tags",
			tags:     map[string]string{"host": "localhost", "link": "http://localhost/bp"},
			expBody: "{\"app_key\":\"key\",\"check\":\"id\",\"description\":\"message\",\"details\":\"details\",\"host\":\"localhost\",\"link\":\"http://localhost/bp\",\"status\":\"ok\",\"task\":\":test\",\"timestamp\":31536038}",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r, err := s.preparePost(
				"id",
				"message",
				"details",
				alert.OK,
				time.Date(1971, 1, 1, 0, 0, 38, 0, time.UTC),
				alert.EventData{
					Name:   "test",
					Tags:   tc.tags,
					Fields: tc.fields,
					Result: models.Result{},
				},
				&HandlerConfig{},
				tc.attrs)

			if err != nil {
				t.Fatal(err)
			}

			buf := new(bytes.Buffer)
			_, err = buf.ReadFrom(r.Body)
			if err != nil {
				t.Fatal(err)
			}
			newStr := buf.String()

			if tc.expBody != newStr {
				t.Fatalf("unexpected content: got '%s' exp '%s'", newStr, tc.expBody)
			}
		})
	}
}

func TestService_Payload(t *testing.T) {
	tags := map[string]string{"host": "localhost", "link": "http://localhost/bp"}
	fields := map[string]interface{}{"count": 10, "load": 0.5}

	testCases := []struct {
		name    string
		config  Config
		attrs   map[string]string
		expBody string
	}{
		{
			name:    "no auto no attributes",
			config:  Config{Enabled: true, AppKey: "key"},
			expBody: "{\"app_key\":\"key\",\"check\":\"id\",\"description\":\"message\",\"details\":\"details\",\"status\":\"ok\",\"task\":\":test\",\"timestamp\":31536038}",
		},
		{
			name:    "no auto with attributes",
			config:  Config{Enabled: true, AppKey: "key"},
			attrs:   map[string]string{"host": "example.com", "link": "http://example.com/bp"},
			expBody: "{\"app_key\":\"key\",\"check\":\"id\",\"description\":\"message\",\"details\":\"details\",\"host\":\"example.com\",\"link\":\"http://example.com/bp\",\"status\":\"ok\",\"task\":\":test\",\"timestamp\":31536038}",
		},
		{
			name:    "auto tags",
			config:  Config{Enabled: true, AppKey: "key", AutoAttributes: "tags"},
			expBody: "{\"app_key\":\"key\",\"check\":\"id\",\"description\":\"message\",\"details\":\"details\",\"host\":\"localhost\",\"link\":\"http://localhost/bp\",\"status\":\"ok\",\"task\":\":test\",\"timestamp\":31536038}",
		},
		{
			name:    "auto fields",
			config:  Config{Enabled: true, AppKey: "key", AutoAttributes: "fields"},
			expBody: "{\"app_key\":\"key\",\"check\":\"id\",\"count\":\"10\",\"description\":\"message\",\"details\":\"details\",\"load\":\"0.5\",\"status\":\"ok\",\"task\":\":test\",\"timestamp\":31536038}",
		},
		{
			name:    "auto tags and fields",
			config:  Config{Enabled: true, AppKey: "key", AutoAttributes: "tags,fields"},
			expBody: "{\"app_key\":\"key\",\"check\":\"id\",\"count\":\"10\",\"description\":\"message\",\"details\":\"details\",\"host\":\"localhost\",\"link\":\"http://localhost/bp\",\"load\":\"0.5\",\"status\":\"ok\",\"task\":\":test\",\"timestamp\":31536038}",
		},
		{
			name:    "auto tags and fields with attributes override",
			config:  Config{Enabled: true, AppKey: "key", AutoAttributes: "tags,fields"},
			attrs:   map[string]string{"host": "example.com", "link": "http://example.com/bp"},
			expBody: "{\"app_key\":\"key\",\"check\":\"id\",\"count\":\"10\",\"description\":\"message\",\"details\":\"details\",\"host\":\"example.com\",\"link\":\"http://example.com/bp\",\"load\":\"0.5\",\"status\":\"ok\",\"task\":\":test\",\"timestamp\":31536038}",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := NewService(tc.config, nil)
			if err != nil {
				t.Fatal(err)
			}
			r, err := s.preparePost(
				"id",
				"message",
				"details",
				alert.OK,
				time.Date(1971, 1, 1, 0, 0, 38, 0, time.UTC),
				alert.EventData{
					Name:   "test",
					Tags:   tags,
					Fields: fields,
					Result: models.Result{},
				},
				&HandlerConfig{},
				tc.attrs)

			if err != nil {
				t.Fatal(err)
			}

			buf := new(bytes.Buffer)
			_, err = buf.ReadFrom(r.Body)
			if err != nil {
				t.Fatal(err)
			}
			newStr := buf.String()

			if tc.expBody != newStr {
				t.Fatalf("unexpected content: got '%s' exp '%s'", newStr, tc.expBody)
			}
		})
	}
}
