package pipeline

// Takes the union of all of its parents.
// The union is just a simple pass through.
// Each data points received from each parent is passed onto children nodes
// without modification.
//
// Example:
//    var logins = stream
//        |from()
//            .measurement('logins')
//    var logouts = stream
//        |from()
//            .measurement('logouts')
//    var frontpage = stream
//        |from()
//            .measurement('frontpage')
//    // Union all user actions into a single stream
//    logins
//        |union(logouts, frontpage)
//            .rename('user_actions')
//        ...
//
type UnionNode struct {
	chainnode
	// The new name of the stream.
	// If empty the name of the left node
	// (i.e. `leftNode.union(otherNode1, otherNode2)`) is used.
	Rename string
}

func newUnionNode(e EdgeType, nodes []Node) *UnionNode {
	u := &UnionNode{
		chainnode: newBasicChainNode("union", e, e),
	}
	for _, n := range nodes {
		n.linkChild(u)
	}
	return u
}
