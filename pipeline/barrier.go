package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/influxdata/influxdb/influxql"
)

// A BarrierNode will emit a barrier with the current time, according to the system
// clock.  Since the BarrierNode emits based on system time, it allows pipelines to be
// forced in the absence of data traffic.  The barrier emitted will be based on either
// idle time since the last received message or on a periodic timer based on the system
// clock.  Any messages received after an emitted barrier that is older than the last
// emitted barrier will be dropped.
//
// Example:
//    stream
//        |barrier().idle(5s)
//        |window()
//            .period(10s)
//            .every(5s)
//        |top(10, 'value')
//        //Post the top 10 results over the last 10s updated every 5s.
//        |httpPost('http://example.com/api/top10')
//
type BarrierNode struct {
	chainnode

	// Emit barrier based on idle time since the last received message.
	// Must be greater than zero.
	Idle time.Duration `json:"idle"`

	// Emit barrier based on periodic timer.  The timer is based on system
	// clock rather than message time.
	// Must be greater than zero.
	Period time.Duration `json:"period"`

	// Delete indicates that the group should be deleted after processing each barrier.
	// This includes the barrier node itself, meaning that if delete is true then the barrier will be triggered
	// only once for each group and then the barrier node will forget about that group.
	// The group will be created again if a new point is received for that group.
	// This is useful if you have increasing cardinality over time as once a barrier is triggered for a group it is then deleted,
	// freeing any resources managing the group.
	Delete bool `json:"delete,omitempty"`
}

func newBarrierNode(wants EdgeType) *BarrierNode {
	return &BarrierNode{
		chainnode: newBasicChainNode("barrier", wants, wants),
	}
}

// tick:ignore
func (b *BarrierNode) validate() error {
	if b.Idle != 0 && b.Period != 0 {
		return errors.New("cannot specify both idle and period")
	}
	if b.Period == 0 && b.Idle <= 0 {
		return errors.New("idle must be greater than zero")
	}
	if b.Period <= 0 && b.Idle == 0 {
		return errors.New("period must be greater than zero")
	}

	return nil
}

// MarshalJSON converts BarrierNode to JSON
// tick:ignore
func (n *BarrierNode) MarshalJSON() ([]byte, error) {
	type Alias BarrierNode
	var raw = &struct {
		TypeOf
		*Alias
		Period string `json:"period"`
		Idle   string `json:"idle"`
	}{
		TypeOf: TypeOf{
			Type: "barrier",
			ID:   n.ID(),
		},
		Alias:  (*Alias)(n),
		Period: influxql.FormatDuration(n.Period),
		Idle:   influxql.FormatDuration(n.Idle),
	}
	return json.Marshal(raw)
}

// UnmarshalJSON converts JSON to an BarrierNode
// tick:ignore
func (n *BarrierNode) UnmarshalJSON(data []byte) error {
	type Alias BarrierNode
	var raw = &struct {
		TypeOf
		*Alias
		Period string `json:"period"`
		Idle   string `json:"idle"`
	}{
		Alias: (*Alias)(n),
	}
	err := json.Unmarshal(data, raw)
	if err != nil {
		return err
	}
	if raw.Type != "barrier" {
		return fmt.Errorf("error unmarshaling node %d of type %s as BarrierNode", raw.ID, raw.Type)
	}

	n.Period, err = influxql.ParseDuration(raw.Period)
	if err != nil {
		return err
	}

	n.Idle, err = influxql.ParseDuration(raw.Idle)
	if err != nil {
		return err
	}

	n.setID(raw.ID)
	return nil
}

//tick:ignore
func (n *BarrierNode) ChainMethods() map[string]reflect.Value {
	return map[string]reflect.Value{
		"Delete": reflect.ValueOf(n.chainnode.Delete),
	}
}
