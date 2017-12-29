package pipeline

import (
	"encoding/json"
	"fmt"
)

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
	chainnode `json:"-"`
	// The new name of the stream.
	// If empty the name of the left node
	// (i.e. `leftNode.union(otherNode1, otherNode2)`) is used.
	Rename string `json:"rename"`
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
// tick:ignore
func (n *UnionNode) MarshalJSON() ([]byte, error) {
	type Alias UnionNode
	var raw = &struct {
		TypeOf
		*Alias
	}{
		TypeOf: TypeOf{
			Type: "union",
			ID:   n.ID(),
		},
		Alias: (*Alias)(n),
	}
	return json.Marshal(raw)
}

// UnmarshalJSON converts JSON to an UnionNode
// tick:ignore
func (n *UnionNode) UnmarshalJSON(data []byte) error {
	type Alias UnionNode
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
	if raw.Type != "union" {
		return fmt.Errorf("error unmarshaling node %d of type %s as UnionNode", raw.ID, raw.Type)
	}
	n.setID(raw.ID)
	return nil
}
