package pipeline

import (
	"encoding/json"
	"fmt"

	"github.com/influxdata/kapacitor/tick/ast"
)

// The WhereNode filters the data stream by a given expression.
//
// Example:
// var sums = stream
//     |from()
//         .groupBy('service', 'host')
//     |sum('value')
// //Watch particular host for issues.
// sums
//    |where(lambda: "host" == 'h001.example.com')
//    |alert()
//        .crit(lambda: TRUE)
//        .email().to('user@example.com')
//
type WhereNode struct {
	chainnode `json:"-"`
	// The expression predicate.
	// tick:ignore
	Lambda *ast.LambdaNode `json:"lambda"`
}

func newWhereNode(wants EdgeType, predicate *ast.LambdaNode) *WhereNode {
	return &WhereNode{
		chainnode: newBasicChainNode("where", wants, wants),
		Lambda:    predicate,
	}
}

// MarshalJSON converts WhereNode to JSON
// tick:ignore
func (n *WhereNode) MarshalJSON() ([]byte, error) {
	type Alias WhereNode
	var raw = &struct {
		TypeOf
		*Alias
	}{
		TypeOf: TypeOf{
			Type: "where",
			ID:   n.ID(),
		},
		Alias: (*Alias)(n),
	}
	return json.Marshal(raw)
}

// UnmarshalJSON converts JSON to an WhereNode
// tick:ignore
func (n *WhereNode) UnmarshalJSON(data []byte) error {
	type Alias WhereNode
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
	if raw.Type != "where" {
		return fmt.Errorf("error unmarshaling node %d of type %s as WhereNode", raw.ID, raw.Type)
	}
	n.setID(raw.ID)
	return nil
}
