package stateful

import (
	"fmt"
	"regexp"
	"time"

	"github.com/influxdata/kapacitor/tick/ast"
)

type EvalUnaryNode struct {
	nodeEvaluator   NodeEvaluator
	constReturnType ast.ValueType
}

func NewEvalUnaryNode(unaryNode *ast.UnaryNode) (*EvalUnaryNode, error) {
	if !isValidUnaryOperator(unaryNode.Operator) {
		return nil, fmt.Errorf("Invalid unary operator: %q", unaryNode.Operator)
	}

	nodeEvaluator, err := createNodeEvaluator(unaryNode.Node)
	if err != nil {
		return nil, fmt.Errorf("Failed to handle node: %v", err)
	}

	return &EvalUnaryNode{
		nodeEvaluator:   nodeEvaluator,
		constReturnType: getConstantNodeType(unaryNode),
	}, nil
}

func isValidUnaryOperator(operator ast.TokenType) bool {
	return operator == ast.TokenNot || operator == ast.TokenMinus
}

func (n *EvalUnaryNode) Type(scope ReadOnlyScope, executionState ExecutionState) (ast.ValueType, error) {
	if n.constReturnType == ast.InvalidType {
		// We are dynamic and we need to figure out our type
		// Do NOT cache this result in n.returnType since it can change.
		return n.nodeEvaluator.Type(scope, executionState)
	}
	return n.constReturnType, nil
}

func (n *EvalUnaryNode) IsDynamic() bool {
	if n.constReturnType != ast.InvalidType {
		return false
	}
	return n.nodeEvaluator.IsDynamic()
}

func (n *EvalUnaryNode) EvalRegex(scope *Scope, executionState ExecutionState) (*regexp.Regexp, error) {
	return nil, ErrTypeGuardFailed{RequestedType: ast.TRegex, ActualType: n.constReturnType}
}

func (n *EvalUnaryNode) EvalTime(scope *Scope, executionState ExecutionState) (time.Time, error) {
	return time.Time{}, ErrTypeGuardFailed{RequestedType: ast.TTime, ActualType: n.constReturnType}
}

func (n *EvalUnaryNode) EvalDuration(scope *Scope, executionState ExecutionState) (time.Duration, error) {
	typ, err := n.Type(scope, executionState)
	if err != nil {
		return 0, err
	}
	if typ == ast.TDuration {
		result, err := n.nodeEvaluator.EvalDuration(scope, executionState)
		if err != nil {
			return 0, err
		}

		return -1 * result, nil
	}

	return 0, ErrTypeGuardFailed{RequestedType: ast.TDuration, ActualType: typ}
}

func (n *EvalUnaryNode) EvalString(scope *Scope, executionState ExecutionState) (string, error) {
	return "", ErrTypeGuardFailed{RequestedType: ast.TString, ActualType: n.constReturnType}
}

func (n *EvalUnaryNode) EvalFloat(scope *Scope, executionState ExecutionState) (float64, error) {
	typ, err := n.Type(scope, executionState)
	if err != nil {
		return 0, err
	}
	if typ == ast.TFloat {
		result, err := n.nodeEvaluator.EvalFloat(scope, executionState)
		if err != nil {
			return 0, err
		}

		return -1 * result, nil
	}

	return 0, ErrTypeGuardFailed{RequestedType: ast.TFloat, ActualType: typ}
}

func (n *EvalUnaryNode) EvalInt(scope *Scope, executionState ExecutionState) (int64, error) {
	typ, err := n.Type(scope, executionState)
	if err != nil {
		return 0, err
	}
	if typ == ast.TInt {
		result, err := n.nodeEvaluator.EvalInt(scope, executionState)
		if err != nil {
			return 0, err
		}

		return -1 * result, nil
	}

	return 0, ErrTypeGuardFailed{RequestedType: ast.TInt, ActualType: typ}
}

func (n *EvalUnaryNode) EvalBool(scope *Scope, executionState ExecutionState) (bool, error) {
	typ, err := n.Type(scope, executionState)
	if err != nil {
		return false, err
	}
	if typ == ast.TBool {
		result, err := n.nodeEvaluator.EvalBool(scope, executionState)
		if err != nil {
			return false, err
		}

		return !result, nil
	}

	return false, ErrTypeGuardFailed{RequestedType: ast.TBool, ActualType: typ}
}
