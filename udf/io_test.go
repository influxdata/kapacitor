package udf_test

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/influxdata/kapacitor/udf"
)

func TestMessage_ReadWrite(t *testing.T) {
	req := &udf.Request{}
	req.Message = &udf.Request_Keepalive{
		Keepalive: &udf.KeepaliveRequest{
			Time: 42,
		},
	}

	var buf bytes.Buffer

	err := udf.WriteMessage(req, &buf)
	if err != nil {
		t.Fatal(err)
	}

	nreq := &udf.Request{}
	var b []byte
	err = udf.ReadMessage(&b, &buf, nreq)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(req, nreq) {
		t.Errorf("unexpected request: \ngot %v\nexp %v", nreq, req)
	}
}

func TestMessage_ReadWriteMultiple(t *testing.T) {
	req := &udf.Request{}
	req.Message = &udf.Request_Keepalive{
		Keepalive: &udf.KeepaliveRequest{
			Time: 42,
		},
	}

	var buf bytes.Buffer

	var count int = 1e4
	for i := 0; i < count; i++ {
		err := udf.WriteMessage(req, &buf)
		if err != nil {
			t.Fatal(err)
		}
	}

	nreq := &udf.Request{}
	var b []byte

	for i := 0; i < count; i++ {
		err := udf.ReadMessage(&b, &buf, nreq)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(req, nreq) {
			t.Fatalf("unexpected request: i:%d \ngot %v\nexp %v", i, nreq, req)
		}
	}
}
