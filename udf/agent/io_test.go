package agent_test

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/influxdata/kapacitor/udf/agent"
)

func TestMessage_ReadWrite(t *testing.T) {
	req := &agent.Request{}
	req.Message = &agent.Request_Keepalive{
		Keepalive: &agent.KeepaliveRequest{
			Time: 42,
		},
	}

	var buf bytes.Buffer

	err := agent.WriteMessage(req, &buf)
	if err != nil {
		t.Fatal(err)
	}

	nreq := &agent.Request{}
	var b []byte
	err = agent.ReadMessage(&b, &buf, nreq)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(req, nreq) {
		t.Errorf("unexpected request: \ngot %v\nexp %v", nreq, req)
	}
}

func TestMessage_ReadWriteMultiple(t *testing.T) {
	req := &agent.Request{}
	req.Message = &agent.Request_Keepalive{
		Keepalive: &agent.KeepaliveRequest{
			Time: 42,
		},
	}

	var buf bytes.Buffer

	var count int = 1e4
	for i := 0; i < count; i++ {
		err := agent.WriteMessage(req, &buf)
		if err != nil {
			t.Fatal(err)
		}
	}

	nreq := &agent.Request{}
	var b []byte

	for i := 0; i < count; i++ {
		err := agent.ReadMessage(&b, &buf, nreq)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(req, nreq) {
			t.Fatalf("unexpected request: i:%d \ngot %v\nexp %v", i, nreq, req)
		}
	}
}
