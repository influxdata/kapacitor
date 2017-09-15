package pipeline

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestQueryNode(t *testing.T) {
	want := newQueryNode().
		GroupBy("host").
		GroupByMeasurement().
		Align().
		AlignGroup()
	want.QueryStr = (`SELECT * from "telegraf"."autogen"`)
	want.Period = time.Second
	want.Every = time.Second
	want.Cron = "0 0 29 2 *"
	want.Offset = time.Second
	want.Fill = 1.0
	want.Cluster = "string"

	b, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("TestQueryNode error while marshaling to JSON: %v", err)
	}
	if b == nil {
		t.Fatal("TestQueryNode error while marshaling to JSON: produced nil")
	}

	var got QueryNode
	if err = json.Unmarshal(b, &got); err != nil {
		t.Fatalf("TestQueryNode error while unmarshaling JSON: %v", err)
	}

	opts := []cmp.Option{cmp.AllowUnexported(QueryNode{}), cmpopts.IgnoreUnexported(QueryNode{})}
	if !cmp.Equal(*want, got, opts...) {
		t.Errorf("TestQueryNode error in JSON serialization. -got/+want\n%s", cmp.Diff(*want, got, opts...))
	}
}
