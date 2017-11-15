package pipeline

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/influxql"
)

// Sample points or batches.
// One point will be emitted every count or duration specified.
//
// Example:
//    stream
//        |sample(3)
//
// Keep every third data point or batch.
//
// Example:
//    stream
//        |sample(10s)
//
// Keep only samples that land on the 10s boundary.
// See FromNode.Truncate, QueryNode.GroupBy time or WindowNode.Align
// for ensuring data is aligned with a boundary.
type SampleNode struct {
	chainnode `json:"-"`

	// Keep every N point or batch
	// tick:ignore
	N int64 `json:"n"`

	// Keep one point or batch every Duration
	// tick:ignore
	Duration time.Duration `json:"duration"`
}

func newSampleNode(wants EdgeType, rate interface{}) *SampleNode {
	var n int64
	var d time.Duration
	switch r := rate.(type) {
	case int64:
		n = r
	case time.Duration:
		d = r
	default:
		panic("must pass int64 or duration to new sample node")
	}

	return &SampleNode{
		chainnode: newBasicChainNode("sample", wants, wants),
		N:         n,
		Duration:  d,
	}
}

// MarshalJSON converts SampleNode to JSON
// tick:ignore
func (n *SampleNode) MarshalJSON() ([]byte, error) {
	type Alias SampleNode
	var raw = &struct {
		TypeOf
		*Alias
		Duration string `json:"duration"`
	}{
		TypeOf: TypeOf{
			Type: "sample",
			ID:   n.ID(),
		},
		Alias:    (*Alias)(n),
		Duration: influxql.FormatDuration(n.Duration),
	}
	return json.Marshal(raw)
}

// UnmarshalJSON converts JSON to an SampleNode
// tick:ignore
func (n *SampleNode) UnmarshalJSON(data []byte) error {
	type Alias SampleNode
	var raw = &struct {
		TypeOf
		*Alias
		Duration string `json:"duration"`
	}{
		Alias: (*Alias)(n),
	}
	err := json.Unmarshal(data, raw)
	if err != nil {
		return err
	}
	if raw.Type != "sample" {
		return fmt.Errorf("error unmarshaling node %d of type %s as SampleNode", raw.ID, raw.Type)
	}
	n.Duration, err = influxql.ParseDuration(raw.Duration)
	if err != nil {
		return err
	}
	n.setID(raw.ID)
	return nil
}
