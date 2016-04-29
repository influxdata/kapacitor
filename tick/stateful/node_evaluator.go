package stateful

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/influxdata/kapacitor/tick"
)

// ErrTypeGuardFailed is returned when a speicifc value type is requested thorugh NodeEvaluator (for example: "Float64Value")
// when the node doesn't support the given type, for example "Float64Value" is called on BoolNode
type ErrTypeGuardFailed struct {
	RequestedType ValueType
	ActualType    ValueType
}

func (e ErrTypeGuardFailed) Error() string {
	return fmt.Sprintf("expression returned unexpected type %s", e.ActualType)
}

type ReadOnlyScope interface {
	Get(name string) (interface{}, error)
}

// NodeEvaluator provides a generic way for trying to fetch
// node value, if a speicifc type is requested (so Value isn't called, the *Value is called) ErrTypeGuardFailed must be returned
type NodeEvaluator interface {
	EvalFloat(scope *tick.Scope, executionState ExecutionState) (float64, error)
	EvalInt(scope *tick.Scope, executionState ExecutionState) (int64, error)
	EvalString(scope *tick.Scope, executionState ExecutionState) (string, error)
	EvalBool(scope *tick.Scope, executionState ExecutionState) (bool, error)
	EvalRegex(scope *tick.Scope, executionState ExecutionState) (*regexp.Regexp, error)

	// Type returns the type of ValueType
	Type(scope ReadOnlyScope, executionState ExecutionState) (ValueType, error)
}

func createNodeEvaluator(n tick.Node) (NodeEvaluator, error) {
	switch node := n.(type) {

	case *tick.BoolNode:
		return &EvalBoolNode{Node: node}, nil

	case *tick.NumberNode:
		switch {
		case node.IsFloat:
			return &EvalFloatNode{Float64: node.Float64}, nil

		case node.IsInt:
			return &EvalIntNode{Int64: node.Int64}, nil

		default:
			// We wouldn't reach ever, unless there is bug in tick parsing ;)
			return nil, errors.New("Invalid NumberNode: Not float or int")
		}

	case *tick.StringNode:
		return &EvalStringNode{Node: node}, nil

	case *tick.RegexNode:
		return &EvalRegexNode{Node: node}, nil

	case *tick.BinaryNode:
		return NewEvalBinaryNode(node)

	case *tick.ReferenceNode:
		return &EvalReferenceNode{Node: node}, nil

	case *tick.FunctionNode:
		return NewEvalFunctionNode(node)

	case *tick.UnaryNode:
		return NewEvalUnaryNode(node)
	}

	return nil, fmt.Errorf("Given node type is not valid evaluation node: %T", n)
}
