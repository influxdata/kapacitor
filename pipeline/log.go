package pipeline

import (
	"encoding/json"
	"fmt"
)

// A node that logs all data that passes through the node.
//
// Example:
//    stream.from()...
//      |window()
//          .period(10s)
//          .every(10s)
//      |log()
//      |count('value')
//
type LogNode struct {
	chainnode

	// The level at which to log the data.
	// One of: DEBUG, INFO, WARN, ERROR
	// Default: INFO
	Level string `json:"level"`
	// Optional prefix to add to all log messages
	Prefix string `json:"prefix"`
}

func newLogNode(wants EdgeType) *LogNode {
	return &LogNode{
		chainnode: newBasicChainNode("log", wants, wants),
		Level:     "INFO",
	}
}

// MarshalJSON converts LogNode to JSON
// tick:ignore
func (n *LogNode) MarshalJSON() ([]byte, error) {
	type Alias LogNode
	var raw = &struct {
		TypeOf
		*Alias
	}{
		TypeOf: TypeOf{
			Type: "log",
			ID:   n.ID(),
		},
		Alias: (*Alias)(n),
	}
	return json.Marshal(raw)
}

// UnmarshalJSON converts JSON to an LogNode
// tick:ignore
func (n *LogNode) UnmarshalJSON(data []byte) error {
	type Alias LogNode
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
	if raw.Type != "log" {
		return fmt.Errorf("error unmarshaling node %d of type %s as LogNode", raw.ID, raw.Type)
	}
	n.setID(raw.ID)
	return nil
}
