package pipeline

// A node that does not perform any operation.
//
// *Do not use this node in a TICKscript there should be no need for it.*
//
// If a node does not have any children, then its emitted count remains zero.
// Using a NoOpNode is a work around so that statistics are accurately reported
// for nodes with no real children.
// A NoOpNode is automatically appended to any node that is a source for a StatsNode
// and does not have any children.
type NoOpNode struct {
	chainnode
}

func newNoOpNode(wants EdgeType) *NoOpNode {
	return &NoOpNode{
		chainnode: newBasicChainNode("noop", wants, wants),
	}
}
