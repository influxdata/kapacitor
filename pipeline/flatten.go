package pipeline

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/influxql"
)

const (
	defaultFlattenDelimiter = "."
)

// Flatten a set of points on specific dimensions.
// For example given two points:
//
// m,host=A,port=80 bytes=3512
// m,host=A,port=443 bytes=6723
//
// Flattening the points on `port` would result in a single point:
//
// m,host=A 80.bytes=3512,443.bytes=6723
//
// Example:
//        |flatten()
//            .on('port')
//
// If flattening on multiple dimensions the order is preserved:
//
// m,host=A,port=80 bytes=3512
// m,host=A,port=443 bytes=6723
// m,host=B,port=443 bytes=7243
//
// Flattening the points on `host` and `port` would result in a single point:
//
// m A.80.bytes=3512,A.443.bytes=6723,B.443.bytes=7243
//
// Example:
//        |flatten()
//            .on('host', 'port')
//
//
// Since flattening points creates dynamically named fields in general it is expected
// that the resultant data is passed to a UDF or similar for custom processing.
type FlattenNode struct {
	chainnode `json:"-"`

	// The dimensions on which to join
	// tick:ignore
	Dimensions []string `tick:"On" json:"on"`

	// The delimiter between field name parts
	Delimiter string `json:"delimiter"`

	// The maximum duration of time that two incoming points
	// can be apart and still be considered to be equal in time.
	// The joined data point's time will be rounded to the nearest
	// multiple of the tolerance duration.
	Tolerance time.Duration `json:"tolerance"`

	// DropOriginalFieldNameFlag indicates whether the original field name should
	// be included in the final field name.
	//tick:ignore
	DropOriginalFieldNameFlag bool `tick:"DropOriginalFieldName" json:"dropOriginalFieldName"`
}

func newFlattenNode(e EdgeType) *FlattenNode {
	f := &FlattenNode{
		chainnode: newBasicChainNode("flatten", e, e),
		Delimiter: defaultFlattenDelimiter,
	}
	return f
}

// MarshalJSON converts FlattenNode to JSON
// tick:ignore
func (n *FlattenNode) MarshalJSON() ([]byte, error) {
	type Alias FlattenNode
	var raw = &struct {
		TypeOf
		*Alias
		Tolerance string `json:"tolerance"`
	}{
		TypeOf: TypeOf{
			Type: "flatten",
			ID:   n.ID(),
		},
		Alias:     (*Alias)(n),
		Tolerance: influxql.FormatDuration(n.Tolerance),
	}
	return json.Marshal(raw)
}

// UnmarshalJSON converts JSON to an FlattenNode
// tick:ignore
func (n *FlattenNode) UnmarshalJSON(data []byte) error {
	type Alias FlattenNode
	var raw = &struct {
		TypeOf
		*Alias
		Tolerance string `json:"tolerance"`
	}{
		Alias: (*Alias)(n),
	}
	err := json.Unmarshal(data, raw)
	if err != nil {
		return err
	}
	if raw.Type != "flatten" {
		return fmt.Errorf("error unmarshaling node %d of type %s as FlattenNode", raw.ID, raw.Type)
	}
	n.Tolerance, err = influxql.ParseDuration(raw.Tolerance)
	if err != nil {
		return err
	}
	n.setID(raw.ID)
	return nil
}

// Specify the dimensions on which to flatten the points.
// tick:property
func (f *FlattenNode) On(dims ...string) *FlattenNode {
	f.Dimensions = dims
	return f
}

// DropOriginalFieldName indicates whether the original field name should
// be dropped when constructing the final field name.
// tick:property
func (f *FlattenNode) DropOriginalFieldName(drop ...bool) *FlattenNode {
	if len(drop) == 1 {
		f.DropOriginalFieldNameFlag = drop[0]
	} else {
		f.DropOriginalFieldNameFlag = true
	}
	return f
}
