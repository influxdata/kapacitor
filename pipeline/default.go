package pipeline

import (
	"encoding/json"
	"fmt"
)

// Defaults fields and tags on data points.
//
// Example:
//    stream
//        |default()
//            .field('value', 0.0)
//            .tag('host', '')
//
// The above example will set the field `value` to float64(0) if it does not already exist
// It will also set the tag `host` to string("") if it does not already exist.
//
// Available Statistics:
//
//    * fields_defaulted -- number of fields that were missing
//    * tags_defaulted -- number of tags that were missing
//
type DefaultNode struct {
	chainnode `json:"-"`

	// Set of fields to default
	// tick:ignore
	Fields map[string]interface{} `tick:"Field" json:"fields"`

	// Set of tags to default
	// tick:ignore
	Tags map[string]string `tick:"Tag" json:"tags"`
}

func newDefaultNode(e EdgeType) *DefaultNode {
	n := &DefaultNode{
		chainnode: newBasicChainNode("default", e, e),
		Fields:    make(map[string]interface{}),
		Tags:      make(map[string]string),
	}
	return n
}

// MarshalJSON converts DefaultNode to JSON
// tick:ignore
func (n *DefaultNode) MarshalJSON() ([]byte, error) {
	type Alias DefaultNode
	var raw = &struct {
		TypeOf
		*Alias
	}{
		TypeOf: TypeOf{
			Type: "default",
			ID:   n.ID(),
		},
		Alias: (*Alias)(n),
	}
	return json.Marshal(raw)
}

// UnmarshalJSON converts JSON to an DefaultNode
// tick:ignore
func (n *DefaultNode) UnmarshalJSON(data []byte) error {
	type Alias DefaultNode
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
	if raw.Type != "default" {
		return fmt.Errorf("error unmarshaling node %d of type %s as DefaultNode", raw.ID, raw.Type)
	}
	n.setID(raw.ID)
	return nil
}

// Define a field default.
// tick:property
func (n *DefaultNode) Field(name string, value interface{}) *DefaultNode {
	n.Fields[name] = value
	return n
}

// Define a tag default.
// tick:property
func (n *DefaultNode) Tag(name string, value string) *DefaultNode {
	n.Tags[name] = value
	return n
}

func (n *DefaultNode) validate() error {
	for field, value := range n.Fields {
		switch value.(type) {
		case float64:
		case int64:
		case bool:
		case string:
		default:
			return fmt.Errorf("unsupported type %T for field %q, field default values must be float,int,string or bool", value, field)
		}
	}
	return nil
}
