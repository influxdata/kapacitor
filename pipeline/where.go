package pipeline

import (
	"bytes"
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
	chainnode
	// The expression predicate.
	// tick:ignore
	Lambda *ast.LambdaNode
}

func newWhereNode(wants EdgeType, predicate *ast.LambdaNode) *WhereNode {
	return &WhereNode{
		chainnode: newBasicChainNode("where", wants, wants),
		Lambda:    predicate,
	}
}

// Tick converts the pipeline node into the TICKScript
func (n *WhereNode) Tick(buf *bytes.Buffer) {
	tick := "|where("
	if n.Lambda != nil {
		tick += LambdaTick(n.Lambda)
	}
	tick += ")"
	buf.Write([]byte(tick))

	for _, child := range n.Children() {
		child.Tick(buf)
	}
}

func (n *WhereNode) MarshalJSON() ([]byte, error) {
	props := map[string]interface{}{
		"type":     "where",
		"nodeID":   fmt.Sprintf("%d", n.ID()),
		"children": n.node,
		"lambda":   n.Lambda,
	}
	return json.Marshal(props)
}
