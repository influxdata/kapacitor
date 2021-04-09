package pipeline

import (
	"encoding/json"
	"fmt"
)

// A node that converts from batchedges to streamedges.
// Example:
//     var errors = stream
//                      |from()
//                      |trickle()
// Children of trickle will be treated as if they are in a stream.
type TrickleNode struct {
	chainnode `json:"-"`
}

func newTrickleNode() *TrickleNode {
	return &TrickleNode{
		chainnode: newBasicChainNode("trickle", BatchEdge, StreamEdge),
	}
}

// MarshalJSON converts TrickleNode to JSON
// tick:ignore
func (n *TrickleNode) MarshalJSON() ([]byte, error) {
	var raw = &struct {
		TypeOf
	}{
		TypeOf: TypeOf{
			Type: "trickle",
			ID:   n.ID(),
		},
	}
	return json.Marshal(raw)
}

// UnmarshalJSON converts JSON to an TrickleNode
// tick:ignore
func (n *TrickleNode) UnmarshalJSON(data []byte) error {
	var raw = &struct {
		TypeOf
	}{}
	err := json.Unmarshal(data, raw)
	if err != nil {
		return err
	}
	if raw.Type != "trickle" {
		return fmt.Errorf("error unmarshaling node %d of type %s as TrickleNode", raw.ID, raw.Type)
	}
	n.setID(raw.ID)
	return nil
}
