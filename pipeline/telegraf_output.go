package pipeline

import (
	"encoding/json"
	"fmt"
)

type TelegrafOutNode struct {
	chainnode `json:"-"`
	toml      string
}

func newTegrafOutNode() *TelegrafOutNode {

	return &TelegrafOutNode{
		chainnode: newBasicChainNode("telegrafOut", BatchEdge, StreamEdge),
	}
}

// MarshalJSON converts TrickleNode to JSON
// tick:ignore
func (n *TelegrafOutNode) MarshalJSON() ([]byte, error) {
	var raw = &struct {
		TypeOf
		TOMLConfig string
	}{
		TypeOf: TypeOf{
			Type: "eval",
			ID:   n.ID(),
		},
		TOMLConfig: n.toml,
	}
	return json.Marshal(raw)
}

// UnmarshalJSON converts JSON to an TrickleNode
// tick:ignore
func (n *TelegrafOutNode) UnmarshalJSON(data []byte) error {
	var raw = &struct {
		TypeOf
		TOMLConfig string
	}{}
	err := json.Unmarshal(data, raw)
	if err != nil {
		return err
	}
	if raw.Type != "telegrafOut" {
		return fmt.Errorf("error unmarshaling node %d of type %s as TelegrafOutNode", raw.ID, raw.Type)
	}
	n.setID(raw.ID)
	return nil
}
