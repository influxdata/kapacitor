package pipeline

import (
	"encoding/json"
	"fmt"
)

// Sideload adds fields and tags to points based on hierarchical data from various sources.
//
// Example:
//        |sideload()
//             .source('file:///path/to/dir')
//             .order('host/{{.host}}.yml', 'hostgroup/{{.hostgroup}}.yml')
//             .field('cpu_threshold', 0.0)
//             .tag('foo', 'unknown')
//
// Add a field `cpu_threshold` and a tag `foo` to each point based on the value loaded from the hierarchical source.
// The list of templates in the `.order()` property are evaluated using the points tags.
// The files paths are checked then checked in order for the specified keys and the first value that is found is used.
type SideloadNode struct {
	chainnode

	// Source for the data, currently only `file://` based sources are supported
	Source string `json:"source"`

	// Order is a list of paths that indicate the hierarchical order.
	// The paths are relative to the source and can have `{}` that will be replaced with the tag value from the point.
	// This allows for values to be overridden based on a hierarchy of tags.
	// tick:ignore
	OrderList []string `tick:"Order" json:"order"`

	// Fields is a list of fields to load.
	// tick:ignore
	Fields map[string]interface{} `tick:"Field" json:"fields"`
	// Tags is a list of tags to load.
	// tick:ignore
	Tags map[string]string `tick:"Tag" json:"tags"`
}

func newSideloadNode(wants EdgeType) *SideloadNode {
	return &SideloadNode{
		chainnode: newBasicChainNode("sideload", wants, wants),
		Fields:    make(map[string]interface{}),
		Tags:      make(map[string]string),
	}
}

// Order is a list of paths that indicate the hierarchical order.
// The paths are relative to the source and can have template markers like `{{.tagname}}` that will be replaced with the tag value of the point.
// The paths are then searched in order for the keys and the first value that is found is used.
// This allows for values to be overridden based on a hierarchy of tags.
// tick:property
func (n *SideloadNode) Order(order ...string) *SideloadNode {
	n.OrderList = order
	return n
}

// Field is the name of a field to load from the source and its default value.
// The type loaded must match the type of the default value.
// Otherwise an error is recorded and the default value is used.
// tick:property
func (n *SideloadNode) Field(f string, v interface{}) *SideloadNode {
	n.Fields[f] = v
	return n
}

// Tag is the name of a tag to load from the source and its default value.
// The loaded values must be strings, otherwise an error is recorded and the default value is used.
// tick:property
func (n *SideloadNode) Tag(t string, v string) *SideloadNode {
	n.Tags[t] = v
	return n
}

// MarshalJSON converts SideloadNode to JSON
// tick:ignore
func (n *SideloadNode) MarshalJSON() ([]byte, error) {
	type Alias SideloadNode
	var raw = &struct {
		TypeOf
		*Alias
	}{
		TypeOf: TypeOf{
			Type: "sideload",
			ID:   n.ID(),
		},
		Alias: (*Alias)(n),
	}
	return json.Marshal(raw)
}

// UnmarshalJSON converts JSON to an SideloadNode
// tick:ignore
func (n *SideloadNode) UnmarshalJSON(data []byte) error {
	type Alias SideloadNode
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
	if raw.Type != "sideload" {
		return fmt.Errorf("error unmarshaling node %d of type %s as SideloadNode", raw.ID, raw.Type)
	}
	n.setID(raw.ID)
	return nil
}
