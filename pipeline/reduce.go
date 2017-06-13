package pipeline

import (
	"fmt"

	"github.com/influxdata/kapacitor/tick/ast"
)

// Reduces all points in a batch down to a single point.
// A list of expressions may be provided and will be evaluated in the order they are given.
// The results of expressions are available to later expressions in the list.
// The result of expressions on each point is also available to expressions on the next
// point as "_previous".
//
// Example:
//    stream
//        |window()
//          .period(10m)
//          .every(5m)
//        |reduce(lambda: "value" + "_previous.sum")
//          .as('sum')
//          .initialValue(0.0)
//
// The above example will return a single data point with the field `sum`.
//
type ReduceNode struct {
	chainnode

	// The name of the field that results from applying the expression.
	// tick:ignore
	AsList []string `tick:"As"`

	// The names of the expressions that should be converted to tags.
	// tick:ignore
	TagsList []string `tick:"Tags"`

	// tick:ignore
	Lambdas []*ast.LambdaNode

	// tick:ignore
	KeepFlag bool `tick:"Keep"`
	// List of fields to keep
	// if empty and KeepFlag is true
	// keep all fields.
	// tick:ignore
	KeepList []string

	// tick:ignore
	QuietFlag bool `tick:"Quiet"`

	// Initial value for "_previous" fields referenced in expressions.
	// tick:ignore
	InitialValue interface{}

	// tick:ignore
	DefaultPointFlag bool `tick:"DefaultPoint"`
}

func newReduceNode(exprs []*ast.LambdaNode) *ReduceNode {
	n := &ReduceNode{
		chainnode: newBasicChainNode("reduce", BatchEdge, BatchEdge),
		Lambdas:   exprs,
	}
	return n
}

func (e *ReduceNode) validate() error {
	if asLen, lambdaLen := len(e.AsList), len(e.Lambdas); asLen != lambdaLen {
		return fmt.Errorf("must specify same number of expressions and .as() names: got %d as names, and %d expressions.", asLen, lambdaLen)
	}
	// Validate tag names exist in As names list.
	for _, tag := range e.TagsList {
		found := false
		for _, as := range e.AsList {
			if tag == as {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("invalid tag name %q, name is not present is .as() names", tag)
		}
	}
	return nil
}

// List of names for each expression.
// The expressions are evaluated in order. The result
// of an expression may be referenced by later expressions
// via the name provided.
//
// Example:
//    stream
//        |reduce(lambda: "value" * "value", lambda: 1.0 / "value2")
//            .as('value2', 'inv_value2')
//
// The above example calculates two fields from the value and names them
// `value2` and `inv_value2` respectively.
//
// tick:property
func (e *ReduceNode) As(names ...string) *ReduceNode {
	e.AsList = names
	return e
}

// Convert the result of an expression into a tag.
// The result must be a string.
// Use the `string()` expression function to convert types.
//
//
// Example:
//    stream
//        |reduce(lambda: string(floor("value" / 10.0)))
//            .as('value_bucket')
//            .tags('value_bucket')
//
// The above example calculates an expression from the field `value`, casts it as a string, and names it `value_bucket`.
// The `value_bucket` expression is then converted from a field on the point to a tag `value_bucket` on the point.
//
// Example:
//    stream
//        |reduce(lambda: string(floor("value" / 10.0)))
//            .as('value_bucket')
//            .tags('value_bucket')
//            .keep('value') // keep the original field `value` as well
//
// The above example calculates an expression from the field `value`, casts it as a string, and names it `value_bucket`.
// The `value_bucket` expression is then converted from a field on the point to a tag `value_bucket` on the point.
// The `keep` property preserves the original field `value`.
// Tags are always kept since creating a tag implies you want to keep it.
//
// tick:property
func (e *ReduceNode) Tags(names ...string) *ReduceNode {
	e.TagsList = names
	return e
}

// If called the existing fields will be preserved in addition
// to the new fields being set.
// If not called then only new fields are preserved. (Tags are
// always preserved regardless how `keep` is used.)
//
// Optionally, intermediate values can be discarded
// by passing a list of field names to be kept.
// Only fields in the list will be retained, the rest will be discarded.
// If no list is given then all fields are retained.
//
// Example:
//    stream
//        |eval(lambda: "value" * "value", lambda: 1.0 / "value2")
//            .as('value2', 'inv_value2')
//            .keep('value', 'inv_value2')
//
// In the above example the original field `value` is preserved.
// The new field `value2` is calculated and used in evaluating
// `inv_value2` but is discarded before the point is sent on to child nodes.
// The resulting point has only two fields: `value` and `inv_value2`.
//
// tick:property
func (e *ReduceNode) Keep(fields ...string) *ReduceNode {
	e.KeepFlag = true
	e.KeepList = fields
	return e
}

// Suppress errors during evaluation.
// tick:property
func (e *ReduceNode) Quiet() *ReduceNode {
	e.QuietFlag = true
	return e
}

func (e *ReduceNode) DefaultPoint() *ReduceNode {
	e.DefaultPointFlag = true
	return e
}
