package pipeline

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/influxql"
)

// A StatsNode emits internal statistics about the another node at a given interval.
//
// The interval represents how often to emit the statistics based on real time.
// This means the interval time is independent of the times of the data points the other node is receiving.
// As a result the StatsNode is a root node in the task pipeline.
//
//
// The currently available internal statistics:
//
//    * emitted -- the number of points or batches this node has sent to its children.
//
// Each stat is available as a field in the data stream.
//
// The stats are in groups according to the original data.
// Meaning that if the source node is grouped by the tag 'host' as an example,
// then the counts are output per host with the appropriate 'host' tag.
// Since its possible for groups to change when crossing a node only the emitted groups
// are considered.
//
// Example:
//     var data = stream
//         |from()...
//     // Emit statistics every 1 minute and cache them via the HTTP API.
//     data
//         |stats(1m)
//         |httpOut('stats')
//     // Continue normal processing of the data stream
//     data...
//
// WARNING: It is not recommended to join the stats stream with the original data stream.
// Since they operate on different clocks you could potentially create a deadlock.
// This is a limitation of the current implementation and may be removed in the future.
type StatsNode struct {
	chainnode `json:"-"`
	// tick:ignore
	SourceNode Node `json:"-"`
	// tick:ignore
	Interval time.Duration `json:"interval"`

	// tick:ignore
	AlignFlag bool `tick:"Align" json:"align"`
}

func newStatsNode(n Node, interval time.Duration) *StatsNode {
	return &StatsNode{
		chainnode:  newBasicChainNode("stats", StreamEdge, StreamEdge),
		SourceNode: n,
		Interval:   interval,
	}
}

// MarshalJSON converts StatsNode to JSON
// tick:ignore
func (n *StatsNode) MarshalJSON() ([]byte, error) {
	type Alias StatsNode
	var raw = &struct {
		TypeOf
		*Alias
		Interval string `json:"interval"`
	}{
		TypeOf: TypeOf{
			Type: "stats",
			ID:   n.ID(),
		},
		Alias:    (*Alias)(n),
		Interval: influxql.FormatDuration(n.Interval),
	}
	return json.Marshal(raw)
}

// UnmarshalJSON converts JSON to an StatsNode
// tick:ignore
func (n *StatsNode) UnmarshalJSON(data []byte) error {
	type Alias StatsNode
	var raw = &struct {
		TypeOf
		*Alias
		Interval string `json:"interval"`
	}{
		Alias: (*Alias)(n),
	}
	err := json.Unmarshal(data, raw)
	if err != nil {
		return err
	}
	if raw.Type != "stats" {
		return fmt.Errorf("error unmarshaling node %d of type %s as StatsNode", raw.ID, raw.Type)
	}
	n.Interval, err = influxql.ParseDuration(raw.Interval)
	if err != nil {
		return err
	}
	n.setID(raw.ID)
	return nil
}

// Round times to the StatsNode.Interval value.
// tick:property
func (n *StatsNode) Align() *StatsNode {
	n.AlignFlag = true
	return n
}
