package stateful

import (
	"fmt"
	"regexp"
	"time"

	"github.com/influxdata/kapacitor/tick/ast"
)

type EvalLambdaNode struct {
	nodeEvaluator   NodeEvaluator
	constReturnType ast.ValueType
	state           ExecutionState
}

func NewEvalLambdaNode(lambda *ast.LambdaNode) (*EvalLambdaNode, error) {
	nodeEvaluator, err := createNodeEvaluator(lambda.Expression)
	if err != nil {
		return nil, fmt.Errorf("Failed to handle node: %v", err)
	}

	return &EvalLambdaNode{
		nodeEvaluator:   nodeEvaluator,
		constReturnType: getConstantNodeType(lambda.Expression),
		// Create an independent state for this expression
		state: CreateExecutionState(),
	}, nil
}

func (n *EvalLambdaNode) Type(scope ReadOnlyScope, _ ExecutionState) (ast.ValueType, error) {
	if n.constReturnType == ast.InvalidType {
		// We are dynamic and we need to figure out our type
		// Do NOT cache this result in n.returnType since it can change.
		return n.nodeEvaluator.Type(scope, n.state)
	}
	return n.constReturnType, nil
}

func (n *EvalLambdaNode) IsDynamic() bool {
	return n.nodeEvaluator.IsDynamic()
}

func (n *EvalLambdaNode) EvalRegex(scope *Scope, _ ExecutionState) (*regexp.Regexp, error) {
	typ, err := n.Type(scope, n.state)
	if err != nil {
		return nil, err
	}
	if typ == ast.TRegex {
		return n.nodeEvaluator.EvalRegex(scope, n.state)
	}

	return nil, ErrTypeGuardFailed{RequestedType: ast.TRegex, ActualType: typ}
}

func (n *EvalLambdaNode) EvalTime(scope *Scope, _ ExecutionState) (time.Time, error) {
	typ, err := n.Type(scope, n.state)
	if err != nil {
		return time.Time{}, err
	}
	return time.Time{}, ErrTypeGuardFailed{RequestedType: ast.TTime, ActualType: typ}
}

func (n *EvalLambdaNode) EvalDuration(scope *Scope, _ ExecutionState) (time.Duration, error) {
	typ, err := n.Type(scope, n.state)
	if err != nil {
		return 0, err
	}
	if typ == ast.TDuration {
		return n.nodeEvaluator.EvalDuration(scope, n.state)
	}

	return 0, ErrTypeGuardFailed{RequestedType: ast.TDuration, ActualType: typ}
}

func (n *EvalLambdaNode) EvalString(scope *Scope, _ ExecutionState) (string, error) {
	typ, err := n.Type(scope, n.state)
	if err != nil {
		return "", err
	}
	if typ == ast.TString {
		return n.nodeEvaluator.EvalString(scope, n.state)
	}

	return "", ErrTypeGuardFailed{RequestedType: ast.TString, ActualType: typ}
}

func (n *EvalLambdaNode) EvalFloat(scope *Scope, _ ExecutionState) (float64, error) {
	typ, err := n.Type(scope, n.state)
	if err != nil {
		return 0, err
	}
	if typ == ast.TFloat {
		return n.nodeEvaluator.EvalFloat(scope, n.state)
	}

	return 0, ErrTypeGuardFailed{RequestedType: ast.TFloat, ActualType: typ}
}

func (n *EvalLambdaNode) EvalInt(scope *Scope, _ ExecutionState) (int64, error) {
	typ, err := n.Type(scope, n.state)
	if err != nil {
		return 0, err
	}
	if typ == ast.TInt {
		return n.nodeEvaluator.EvalInt(scope, n.state)
	}

	return 0, ErrTypeGuardFailed{RequestedType: ast.TInt, ActualType: typ}
}

func (n *EvalLambdaNode) EvalBool(scope *Scope, _ ExecutionState) (bool, error) {
	typ, err := n.Type(scope, n.state)
	if err != nil {
		return false, err
	}
	if typ == ast.TBool {
		return n.nodeEvaluator.EvalBool(scope, n.state)
	}

	return false, ErrTypeGuardFailed{RequestedType: ast.TBool, ActualType: typ}
}
