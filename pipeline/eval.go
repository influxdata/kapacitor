package pipeline

import (
	"github.com/influxdb/kapacitor/tick"
)

// Evaluates expressions on each data point it receives.
// A list of expressions may be provided and will be evaluated in the order they are given
// and results of previous expressions are made available to later expressions.
// See the property 'As' for details on how to reference the results.
//
// Example:
//    stream
//        .eval(lambda: "error_count" / "total_count")
//          .as('error_percent')
//
// The above example will add a new field `error_percent` to each
// data point with the result of `error_count / total_count` where
// `error_count` and `total_count` are existing fields on the data point.
//
type EvalNode struct {
	chainnode

	// The name of the field that results from applying the expression.
	// tick:ignore
	AsList []string

	// tick:ignore
	Expressions []tick.Node

	// tick:ignore
	KeepFlag bool
	// List of fields to keep
	// if empty and KeepFlag is true
	// keep all fields.
	// tick:ignore
	KeepList []string
}

func newEvalNode(e EdgeType, exprs []tick.Node) *EvalNode {
	n := &EvalNode{
		chainnode:   newBasicChainNode("eval", e, e),
		Expressions: exprs,
	}
	return n
}

// List of names for each expression.
// The expressions are evaluated in order and the result
// of a previous will be available in later expressions
// via the name provided.
//
// tick:property
func (e *EvalNode) As(names ...string) *EvalNode {
	e.AsList = names
	return e
}

// If called the existing fields will be preserved in addition
// to the new fields being set.
//
// Optionally a list of field names can be given.
// Only fields in the list will be kept.
// If no list is given then all fields are kept.
// The list of fields to keep is evaluated after all
// expressions have been evaluated.
// This way intermediate values can be discarded.
//
// tick:property
func (e *EvalNode) Keep(fields ...string) *EvalNode {
	e.KeepFlag = true
	e.KeepList = fields
	return e
}
