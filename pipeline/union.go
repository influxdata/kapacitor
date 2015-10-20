package pipeline

// Takes the union of all of its parents.
// The union is just a simple pass through.
// Each data points received from each parent is passed onto children nodes
// without modification.
//
// Example:
//    var logins = stream.fork().from("logins")
//    var logouts = stream.fork().from("logouts")
//    var frontpage = stream.fork().from("frontpage")
//    // Union all user actions into a single stream
//    logins.union(logouts, frontpage)
//            .rename("user_actions")
//        ...
//
type UnionNode struct {
	node
	// The new name of the stream.
	// If empty the name of the left node
	// (i.e. `leftNode.union(otherNode1, otherNode2)`) is used.
	Rename string
}

func newUnionNode(e EdgeType, nodes []Node) *UnionNode {
	u := &UnionNode{
		node: node{
			desc:     "union",
			wants:    e,
			provides: e,
		},
	}
	for _, n := range nodes {
		n.linkChild(u)
	}
	return u
}
