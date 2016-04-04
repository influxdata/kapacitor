package pipeline

import (
	"github.com/influxdata/kapacitor/tick"
)

// Evaluates expressions on each data point it receives.
// A list of expressions may be provided and will be evaluated in the order they are given
// and results of previous expressions are made available to later expressions.
// See the property EvalNode.As for details on how to reference the results.
//
// Example:
//    stream
//        |eval(lambda: "error_count" / "total_count")
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
	AsList []string `tick:"As"`

	// tick:ignore
	Expressions []tick.Node

	// tick:ignore
	KeepFlag bool `tick:"Keep"`
	// List of fields to keep
	// if empty and KeepFlag is true
	// keep all fields.
	// tick:ignore
	KeepList []string

	// tick:ignore
	QuiteFlag bool `tick:"Quiet"`
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
// of a previous expression will be available in later expressions
// via the name provided.
//
// Example:
//    stream
//        |eval(lambda: "value" * "value", lambda: 1.0 / "value2")
//            .as('value2', 'inv_value2')
//
// The above example calculates two fields from the value and names them
// `value2` and `inv_value2` respectively.
//
// tick:property
func (e *EvalNode) As(names ...string) *EvalNode {
	e.AsList = names
	return e
}

// If called the existing fields will be preserved in addition
// to the new fields being set.
// If not called then only new fields are preserved.
//
// Optionally intermediate values can be discarded
// by passing a list of field names.
// Only fields in the list will be kept.
// If no list is given then all fields, new and old, are kept.
//
// Example:
//    stream
//        |eval(lambda: "value" * "value", lambda: 1.0 / "value2")
//            .as('value2', 'inv_value2')
//            .keep('value', 'inv_value2')
//
// In the above example the original field `value` is preserved.
// In addition the new field `value2` is calculated and used in evaluating
// `inv_value2` but is discarded before the point is sent on to children nodes.
// The resulting point has only two fields `value` and `inv_value2`.
// tick:property
func (e *EvalNode) Keep(fields ...string) *EvalNode {
	e.KeepFlag = true
	e.KeepList = fields
	return e
}

// Suppress errors during evaluation.
// tick:property
func (e *EvalNode) Quiet() *EvalNode {
	e.QuiteFlag = true
	return e
}
