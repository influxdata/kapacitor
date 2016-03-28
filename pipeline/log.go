package pipeline

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
