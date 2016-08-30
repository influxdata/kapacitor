package pipeline

import (
	"fmt"

	"github.com/influxdata/kapacitor/tick/ast"
)

// Evaluates expressions on each data point it receives.
// A list of expressions may be provided and will be evaluated in the order they are given.
// The results of expressions are available to later expressions in the list.
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
// Available Statistics:
//
//    * eval_errors -- number of errors evaluating any expressions.
//
type EvalNode struct {
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
	QuiteFlag bool `tick:"Quiet"`
}

func newEvalNode(e EdgeType, exprs []*ast.LambdaNode) *EvalNode {
	n := &EvalNode{
		chainnode: newBasicChainNode("eval", e, e),
		Lambdas:   exprs,
	}
	return n
}

func (e *EvalNode) validate() error {
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

// Convert the result of an expression into a tag.
// The result must be a string.
// Use the `string()` expression function to convert types.
//
//
// Example:
//    stream
//        |eval(lambda: string(floor("value" / 10.0)))
//            .as('value_bucket')
//            .tags('value_bucket')
//
// The above example calculates an expression from the field `value`, casts it as a string, and names it `value_bucket`.
// The `value_bucket` expression is then converted from a field on the point to a tag `value_bucket` on the point.
//
// Example:
//    stream
//        |eval(lambda: string(floor("value" / 10.0)))
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
func (e *EvalNode) Tags(names ...string) *EvalNode {
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
