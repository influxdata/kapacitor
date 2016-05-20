package stateful

import (
	"fmt"
	"regexp"
	"time"

	"github.com/influxdata/kapacitor/tick"
)

type EvalUnaryNode struct {
	nodeEvaluator NodeEvaluator
	returnType    ValueType
}

func NewEvalUnaryNode(unaryNode *tick.UnaryNode) (*EvalUnaryNode, error) {
	if !isValidUnaryOperator(unaryNode.Operator) {
		return nil, fmt.Errorf("Invalid unary operator: %q", unaryNode.Operator)
	}

	nodeEvaluator, err := createNodeEvaluator(unaryNode.Node)
	if err != nil {
		return nil, fmt.Errorf("Failed to handle node: %v", err)
	}

	return &EvalUnaryNode{
		nodeEvaluator: nodeEvaluator,
		returnType:    getConstantNodeType(unaryNode),
	}, nil
}

func isValidUnaryOperator(operator tick.TokenType) bool {
	return operator == tick.TokenNot || operator == tick.TokenMinus
}

func (n *EvalUnaryNode) Type(scope ReadOnlyScope, executionState ExecutionState) (ValueType, error) {
	return findNodeTypes(n.returnType, []NodeEvaluator{n.nodeEvaluator}, scope, executionState)
}

func (n *EvalUnaryNode) EvalRegex(scope *tick.Scope, executionState ExecutionState) (*regexp.Regexp, error) {
	return nil, ErrTypeGuardFailed{RequestedType: TRegex, ActualType: n.returnType}
}

func (n *EvalUnaryNode) EvalTime(scope *tick.Scope, executionState ExecutionState) (time.Time, error) {
	return time.Time{}, ErrTypeGuardFailed{RequestedType: TTime, ActualType: n.returnType}
}

func (n *EvalUnaryNode) EvalString(scope *tick.Scope, executionState ExecutionState) (string, error) {
	return "", ErrTypeGuardFailed{RequestedType: TString, ActualType: n.returnType}
}

func (n *EvalUnaryNode) EvalFloat(scope *tick.Scope, executionState ExecutionState) (float64, error) {
	if n.returnType.IsNumeric() {
		result, err := n.nodeEvaluator.EvalFloat(scope, executionState)
		if err != nil {
			return float64(0), err
		}

		return -1 * result, nil
	}

	return float64(0), ErrTypeGuardFailed{RequestedType: TFloat64, ActualType: n.returnType}
}

func (n *EvalUnaryNode) EvalInt(scope *tick.Scope, executionState ExecutionState) (int64, error) {
	if n.returnType.IsNumeric() {
		result, err := n.nodeEvaluator.EvalInt(scope, executionState)
		if err != nil {
			return int64(0), err
		}

		return -1 * result, nil
	}

	return int64(0), ErrTypeGuardFailed{RequestedType: TInt64, ActualType: n.returnType}
}

func (n *EvalUnaryNode) EvalBool(scope *tick.Scope, executionState ExecutionState) (bool, error) {
	if n.returnType == TBool {
		result, err := n.nodeEvaluator.EvalBool(scope, executionState)
		if err != nil {
			return false, err
		}

		return !result, nil
	}

	return false, ErrTypeGuardFailed{RequestedType: TBool, ActualType: n.returnType}
}
