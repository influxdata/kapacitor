package pipeline

import (
	"encoding/json"

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
	chainnode
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
func (w *WhereNode) MarshalJSON() ([]byte, error) {
	props := JSONNode{}.
		SetType("where").
		SetID(w.ID()).
		Set("lambda", w.Lambda)

	return json.Marshal(&props)
}

func (w *WhereNode) unmarshal(props JSONNode) error {
	err := props.CheckTypeOf("where")
	if err != nil {
		return err
	}

	if w.id, err = props.ID(); err != nil {
		return err
	}

	if w.Lambda, err = props.Lambda("lambda"); err != nil {
		return err
	}
	return nil
}

// UnmarshalJSON converts JSON to WhereNode
func (w *WhereNode) UnmarshalJSON(data []byte) error {
	props, err := NewJSONNode(data)
	if err != nil {
		return err
	}
	return w.unmarshal(props)
}
