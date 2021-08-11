package bigpanda

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/models"
)

func TestService_SerializeEventData(t *testing.T) {
	config := Config{Enabled: true, AppKey: "key"}
	s, err := NewService(config, nil)
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		fields  map[string]interface{}
		expBody string
	}{
		{
			fields:  map[string]interface{}{"primitive_type": 10},
			expBody: "{\"app_key\":\"key\",\"check\":\"id\",\"description\":\"message\",\"details\":\"details\",\"primitive_type\":\"10\",\"status\":\"ok\",\"task\":\":test\",\"timestamp\":31536038}",
		},
		{
			fields:  map[string]interface{}{"primitive_type": "string"},
			expBody: "{\"app_key\":\"key\",\"check\":\"id\",\"description\":\"message\",\"details\":\"details\",\"primitive_type\":\"string\",\"status\":\"ok\",\"task\":\":test\",\"timestamp\":31536038}",
		},
		{
			fields:  map[string]interface{}{"primitive_type": true},
			expBody: "{\"app_key\":\"key\",\"check\":\"id\",\"description\":\"message\",\"details\":\"details\",\"primitive_type\":\"true\",\"status\":\"ok\",\"task\":\":test\",\"timestamp\":31536038}",
		},
		{
			fields:  map[string]interface{}{"primitive_type": 123.45},
			expBody: "{\"app_key\":\"key\",\"check\":\"id\",\"description\":\"message\",\"details\":\"details\",\"primitive_type\":\"123.45\",\"status\":\"ok\",\"task\":\":test\",\"timestamp\":31536038}",
		},
		{
			fields:  map[string]interface{}{"escape": "\n"},
			expBody: "{\"app_key\":\"key\",\"check\":\"id\",\"description\":\"message\",\"details\":\"details\",\"escape\":\"\\n\",\"status\":\"ok\",\"task\":\":test\",\"timestamp\":31536038}",
		},
		{
			fields:  map[string]interface{}{"array": []interface{}{10, true, "string value"}},
			expBody: "{\"app_key\":\"key\",\"array\":\"[10,true,\\\"string value\\\"]\",\"check\":\"id\",\"description\":\"message\",\"details\":\"details\",\"status\":\"ok\",\"task\":\":test\",\"timestamp\":31536038}",
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			r, err := s.preparePost(
				"id",
				"message",
				"details",
				alert.OK,
				time.Date(1971, 1, 1, 0, 0, 38, 0, time.UTC),
				alert.EventData{
					Name:   "test",
					Tags:   make(map[string]string),
					Fields: tc.fields,
					Result: models.Result{},
				},
				&HandlerConfig{})

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
