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
	Level string
	// Optional prefix to add to all log messages
	Prefix string
}

func newLogNode(wants EdgeType) *LogNode {
	return &LogNode{
		chainnode: newBasicChainNode("log", wants, wants),
		Level:     "INFO",
	}
}

func (l *LogNode) MarshalJSON() ([]byte, error) {
	props := map[string]interface{}{
		"type":     "log",
		"nodeID":   fmt.Sprintf("%d", l.ID()),
		"children": l.node,
		"level":    l.Level,
		"prefix":   l.Prefix,
	}
	return json.Marshal(props)
}
