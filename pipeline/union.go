package pipeline

import "encoding/json"

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

// MarshalJSON converts UnionNode to JSON
func (n *UnionNode) MarshalJSON() ([]byte, error) {
	props := JSONNode{}.
		SetType("union").
		SetID(n.ID()).
		Set("rename", n.Rename)

	return json.Marshal(&props)
}

func (n *UnionNode) unmarshal(props JSONNode) error {
	err := props.CheckTypeOf("union")
	if err != nil {
		return err
	}

	if n.id, err = props.ID(); err != nil {
		return err
	}

	if n.Rename, err = props.String("rename"); err != nil {
		return err
	}
	return nil
}

// UnmarshalJSON converts JSON to UnionNode
func (n *UnionNode) UnmarshalJSON(data []byte) error {
	props, err := NewJSONNode(data)
	if err != nil {
		return err
	}
	return n.unmarshal(props)
}
