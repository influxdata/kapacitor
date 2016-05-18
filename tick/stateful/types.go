package stateful

import (
	"reflect"
	"regexp"

	"github.com/influxdata/kapacitor/tick"
)

type ValueType uint8

const (
	InvalidType ValueType = iota << 1
	TFloat64
	TInt64
	TString
	TBool
	TRegex
)

const TNumeric = TFloat64 & TInt64

func (v ValueType) IsNumeric() bool {
	return (v|TInt64 == TInt64) || (v|TFloat64 == TFloat64)
}

func (v ValueType) String() string {
	switch v {
	case TFloat64:
		return "float64"
	case TInt64:
		return "int64"
	case TString:
		return "string"
	case TBool:
		return "boolean"
	case TRegex:
		return "regex"
	}

	return "invalid type"
}

func valueTypeOf(t reflect.Type) ValueType {
	if t == nil {
		return InvalidType
	}
	switch t.Kind() {
	case reflect.Float64:
		return TFloat64
	case reflect.Int64:
		return TInt64
	case reflect.String:
		return TString
	case reflect.Bool:
		return TBool
	default:
		// This is not primitive type.. so it must be regex
		if t == reflect.TypeOf((*regexp.Regexp)(nil)) {
			return TRegex
		}
		return InvalidType
	}
}

// getCostantNodeType - Given a tick.Node we want to know it's return type
// this method does exactly this, few examples:
// *) StringNode -> TString
// *) UnaryNode -> we base the type by the node type
func getConstantNodeType(n tick.Node) ValueType {
	switch node := n.(type) {
	case *tick.NumberNode:
		if node.IsInt {
			return TInt64
		}

		if node.IsFloat {
			return TFloat64
		}
	case *tick.StringNode:
		return TString
	case *tick.BoolNode:
		return TBool
	case *tick.RegexNode:
		return TRegex

	case *tick.UnaryNode:
		// If this is comparison operator we know for sure the output must be boolean
		if node.Operator == tick.TokenNot {
			return TBool
		}

		// if this is math result it can be Int64/Float64, the type decision will be upon
		// getConstantNodeType to choose or if it's dynamic node the EvalUnaryNode will choose.
		if node.Operator == tick.TokenMinus {
			nodeType := getConstantNodeType(node.Node)
			if nodeType == InvalidType {
				return TNumeric
			}

			return nodeType
		}

	case *tick.BinaryNode:
		if node.Operator == tick.TokenPlus {
			leftType := getConstantNodeType(node.Left)
			rightType := getConstantNodeType(node.Right)
			if leftType == TString || rightType == TString {
				return TString
			}
		}
		if tick.IsCompOperator(node.Operator) {
			return TBool
		}

		if tick.IsMathOperator(node.Operator) {
			// Numeric result shall return, we must be more specific
			leftType := getConstantNodeType(node.Left)

			// quick exit here..
			if leftType == TFloat64 {
				return TFloat64
			}

			rightType := getConstantNodeType(node.Right)
			if rightType == TFloat64 {
				return TFloat64
			}

			if leftType == TInt64 && rightType == TInt64 {
				return TInt64
			}

			// This is math operator but now float or int,
			// This is invalid expression which will get an error while evaluating
			// We can return an error right here if we want
			return TNumeric
		}
	}

	return InvalidType
}

func isDynamicNode(n tick.Node) bool {
	switch node := n.(type) {
	case *tick.ReferenceNode:
		return true

	case *tick.FunctionNode:
		return true

	case *tick.UnaryNode:
		// unary if dynamic only if it's childs are dynamic
		return isDynamicNode(node.Node)

	default:
		return false
	}

}

// findNodeTypes returns the aggregative type of the nodes (it handles corner case like TNumeric)
// should be used by EvalUnaryNode and EvalBinaryNode, won't work well for EvalFunctionNode on it's arg
func findNodeTypes(constantType ValueType, nodes []NodeEvaluator, scope ReadOnlyScope, executionState ExecutionState) (ValueType, error) {
	// Generic eval for binary node is a bit tricky! (look at getConstantNodeType before reading this comment!)
	// If this is comparison node we know for sure this is bool return
	// but if this math operator we can't know it's type if it contain dynamic node (for example "value" - int64(5), the result can be int64 or float64)
	// only in the specialization we will know

	// TODO: Fold getConstantType to here
	switch constantType {
	case TBool:
	case TInt64:
	case TFloat64:
		return constantType, nil

	case TNumeric:
		// !! DON'T CACHE THE RESULT in n.Type !!
		// This section of code is very tempting to save the type in n.Type
		// the reason we reached to TNumeric because one side is dynmic, if it's dynamic
		// he can change types, so we.

		// TNumeric means this is TInt64 or TFloat64
		// Enough one is float64, exit , otherwise all are int64
		for _, nodeEvaluator := range nodes {
			nodeType, err := nodeEvaluator.Type(scope, executionState)
			if err != nil {
				return InvalidType, err
			}

			if nodeType == TFloat64 {
				return TFloat64, nil
			}
		}

		return TInt64, nil
	}

	return constantType, nil
}
