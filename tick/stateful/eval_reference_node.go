package stateful

import (
	"fmt"
	"regexp"
	"time"

	"github.com/influxdata/kapacitor/tick/ast"
)

type EvalReferenceNode struct {
	Node *ast.ReferenceNode
}

// getReferenceValue - core method for evaluating function where all NodeEvaluator methods should use
func (n *EvalReferenceNode) getReferenceValue(scope *Scope) (interface{}, error) {
	value, err := scope.Get(n.Node.Reference)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, fmt.Errorf("referenced value %q is nil.", n.Node.Reference)
	}

	return value, nil
}

func (n *EvalReferenceNode) String() string {
	return "\"" + n.Node.Reference + "\""
}

func (n *EvalReferenceNode) Type(scope ReadOnlyScope) (ast.ValueType, error) {
	value, err := n.getReferenceValue(scope.(*Scope))
	if err != nil {
		return ast.InvalidType, err
	}

	return ast.TypeOf(value), nil
}

func (n *EvalReferenceNode) IsDynamic() bool {
	return true
}

func (n *EvalReferenceNode) EvalRegex(scope *Scope, executionState ExecutionState) (*regexp.Regexp, error) {
	refValue, err := n.getReferenceValue(scope)
	if err != nil {
		return nil, err
	}

	if regexValue, isRegex := refValue.(*regexp.Regexp); isRegex {
		return regexValue, nil
	}

	refType := ast.TypeOf(refValue)
	if refType == ast.TMissing {
		return nil, fmt.Errorf("reference \"%s\" is missing value", n.Node.Reference)
	}

	return nil, ErrTypeGuardFailed{RequestedType: ast.TRegex, ActualType: ast.TypeOf(refValue)}
}

func (n *EvalReferenceNode) EvalTime(scope *Scope, executionState ExecutionState) (time.Time, error) {
	refValue, err := n.getReferenceValue(scope)
	if err != nil {
		return time.Time{}, err
	}

	if timeValue, isTime := refValue.(time.Time); isTime {
		return timeValue, nil
	}

	refType := ast.TypeOf(refValue)
	if refType == ast.TMissing {
		return time.Time{}, fmt.Errorf("reference \"%s\" is missing value", n.Node.Reference)
	}

	return time.Time{}, ErrTypeGuardFailed{RequestedType: ast.TTime, ActualType: ast.TypeOf(refValue)}
}

func (n *EvalReferenceNode) EvalDuration(scope *Scope, executionState ExecutionState) (time.Duration, error) {
	refValue, err := n.getReferenceValue(scope)
	if err != nil {
		return 0, err
	}

	if durValue, isDuration := refValue.(time.Duration); isDuration {
		return durValue, nil
	}

	refType := ast.TypeOf(refValue)
	if refType == ast.TMissing {
		return 0, fmt.Errorf("reference \"%s\" is missing value", n.Node.Reference)
	}

	return 0, ErrTypeGuardFailed{RequestedType: ast.TDuration, ActualType: ast.TypeOf(refValue)}
}

func (n *EvalReferenceNode) EvalString(scope *Scope, executionState ExecutionState) (string, error) {
	refValue, err := n.getReferenceValue(scope)
	if err != nil {
		return "", err
	}

	if stringValue, isString := refValue.(string); isString {
		return stringValue, nil
	}

	refType := ast.TypeOf(refValue)
	if refType == ast.TMissing {
		return "", fmt.Errorf("reference \"%s\" is missing value", n.Node.Reference)
	}

	return "", ErrTypeGuardFailed{RequestedType: ast.TString, ActualType: ast.TypeOf(refValue)}
}

func (n *EvalReferenceNode) EvalFloat(scope *Scope, executionState ExecutionState) (float64, error) {
	refValue, err := n.getReferenceValue(scope)
	if err != nil {
		return float64(0), err
	}

	if float64Value, isFloat64 := refValue.(float64); isFloat64 {
		return float64Value, nil
	}

	refType := ast.TypeOf(refValue)
	if refType == ast.TMissing {
		return float64(0), fmt.Errorf("reference \"%s\" is missing value", n.Node.Reference)
	}

	return float64(0), ErrTypeGuardFailed{RequestedType: ast.TFloat, ActualType: ast.TypeOf(refValue)}
}

func (n *EvalReferenceNode) EvalInt(scope *Scope, executionState ExecutionState) (int64, error) {
	refValue, err := n.getReferenceValue(scope)
	if err != nil {
		return int64(0), err
	}

	if int64Value, isInt64 := refValue.(int64); isInt64 {
		return int64Value, nil
	}

	refType := ast.TypeOf(refValue)
	if refType == ast.TMissing {
		return int64(0), fmt.Errorf("reference \"%s\" is missing value", n.Node.Reference)
	}

	return int64(0), ErrTypeGuardFailed{RequestedType: ast.TInt, ActualType: ast.TypeOf(refValue)}
}

func (n *EvalReferenceNode) EvalBool(scope *Scope, executionState ExecutionState) (bool, error) {
	refValue, err := n.getReferenceValue(scope)
	if err != nil {
		return false, err
	}

	if boolValue, isBool := refValue.(bool); isBool {
		return boolValue, nil
	}

	refType := ast.TypeOf(refValue)
	if refType == ast.TMissing {
		return false, fmt.Errorf("reference \"%s\" is missing value", n.Node.Reference)
	}

	return false, ErrTypeGuardFailed{RequestedType: ast.TBool, ActualType: ast.TypeOf(refValue)}
}

func (n *EvalReferenceNode) EvalMissing(scope *Scope, executionState ExecutionState) (*ast.Missing, error) {
	refValue, err := n.getReferenceValue(scope)
	if err != nil {
		return nil, err
	}

	if missingVal, isMissing := refValue.(*ast.Missing); isMissing {
		//  This error gets checked in the eval method of a function node
		return missingVal, fmt.Errorf("missing value: \"%v\"", n.Node.Reference)
	}

	return nil, ErrTypeGuardFailed{RequestedType: ast.TMissing, ActualType: ast.TypeOf(refValue)}
}
