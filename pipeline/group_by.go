package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/influxdata/kapacitor/tick/ast"
)

// A GroupByNode will group the incoming data.
// Each group is then processed independently for the rest of the pipeline.
// Only tags that are dimensions in the grouping will be preserved;
// all other tags are dropped.
//
// Example:
//    stream
//        |groupBy('service', 'datacenter')
//        ...
//
// The above example groups the data along two dimensions `service` and `datacenter`.
// Groups are dynamically created as new data arrives and each group is processed
// independently.
type GroupByNode struct {
	chainnode
	//The dimensions by which to group to the data.
	// tick:ignore
	Dimensions []interface{} `json:"dimensions"`

	// The dimensions to exclude.
	// Useful for substractive tags from using *.
	// tick:ignore
	ExcludedDimensions []string `tick:"Exclude" json:"exclude"`

	// Whether to include the measurement in the group ID.
	// tick:ignore
	ByMeasurementFlag bool `tick:"ByMeasurement" json:"byMeasurement"`
}

func newGroupByNode(wants EdgeType, dims []interface{}) *GroupByNode {
	return &GroupByNode{
		chainnode:  newBasicChainNode("groupby", wants, wants),
		Dimensions: dims,
	}
}

// MarshalJSON converts GroupByNode to JSON
// tick:ignore
func (n *GroupByNode) MarshalJSON() ([]byte, error) {
	type Alias GroupByNode
	var raw = &struct {
		TypeOf
		*Alias
	}{
		TypeOf: TypeOf{
			Type: "groupBy",
			ID:   n.ID(),
		},
		Alias: (*Alias)(n),
	}
	return json.Marshal(raw)
}

// UnmarshalJSON converts JSON to an GroupByNode
// tick:ignore
func (n *GroupByNode) UnmarshalJSON(data []byte) error {
	type Alias GroupByNode
	var raw = &struct {
		TypeOf
		*Alias
	}{
		Alias: (*Alias)(n),
	}
	err := json.Unmarshal(data, raw)
	if err != nil {
		return err
	}
	if raw.Type != "groupBy" {
		return fmt.Errorf("error unmarshaling node %d of type %s as GroupByNode", raw.ID, raw.Type)
	}
	n.setID(raw.ID)
	return nil
}

func (n *GroupByNode) validate() error {
	return validateDimensions(n.Dimensions, n.ExcludedDimensions)
}

func validateDimensions(dimensions []interface{}, excludedDimensions []string) error {
	hasStar := false
	for _, d := range dimensions {
		switch dim := d.(type) {
		case string:
			if len(dim) == 0 {
				return errors.New("dimensions cannot not be the empty string")
			}
		case *ast.StarNode:
			hasStar = true
		default:
			return fmt.Errorf("invalid dimension object of type %T", d)
		}
	}
	if hasStar && len(dimensions) > 1 {
		return errors.New("cannot group by both '*' and named dimensions.")
	}
	if !hasStar && len(excludedDimensions) > 0 {
		return errors.New("exclude requires '*'")
	}
	return nil
}

// If set will include the measurement name in the group ID.
// Along with any other group by dimensions.
//
// Example:
//     ...
//     |groupBy('host')
//         .byMeasurement()
//
// The above example groups points by their host tag and measurement name.
//
// If you want to remove the measurement name from the group ID,
// then groupBy all existing dimensions but without specifying 'byMeasurement'.
//
// Example:
//    |groupBy(*)
//
// The above removes the group by measurement name if any.
// tick:property
func (n *GroupByNode) ByMeasurement() *GroupByNode {
	n.ByMeasurementFlag = true
	return n
}

// Exclude removes any tags from the group.
func (n *GroupByNode) Exclude(dims ...string) *GroupByNode {
	n.ExcludedDimensions = append(n.ExcludedDimensions, dims...)
	return n
}
