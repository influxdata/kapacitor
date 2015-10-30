package pipeline

import (
	"github.com/influxdb/kapacitor/tick"
)

// Evaluates a transformation on each data point it receives.
//
// Example:
//    stream
//        .eval(lambda: "error_count" / "total_count")
//          .as('error_percent')
//
// The above example will add a new field `error_percent` to each
// data point with the result of `error_count / total_count` where
// `error_count` and `total_count` are existing fields on the data point.
type EvalNode struct {
	chainnode

	// The name of the field that results from applying the expression.
	As string

	// tick:ignore
	Expression tick.Node

	// tick:ignore
	KeepFlag bool
}

func newEvalNode(e EdgeType, expr tick.Node) *EvalNode {
	n := &EvalNode{
		chainnode:  newBasicChainNode("eval", e, e),
		Expression: expr,
	}
	return n
}

// If called the existing fields will be preserved in addition
// to the new field being set.
//tick:property
func (e *EvalNode) Keep() *EvalNode {
	e.KeepFlag = true
	return e
}
