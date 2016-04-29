package stateful

import (
	"reflect"
	"regexp"

	"github.com/influxdata/kapacitor/tick"
)

type EvalReferenceNode struct {
	Node *tick.ReferenceNode
}

// getReferenceValue - core method for evaluating function where all NodeEvaluator methods should use
func (n *EvalReferenceNode) getReferenceValue(scope *tick.Scope, executionState ExecutionState) (interface{}, error) {
	value, err := scope.Get(n.Node.Reference)
	if err != nil {
		return nil, err
	}

	return value, nil
}

func (n *EvalReferenceNode) Type(scope ReadOnlyScope, executionState ExecutionState) (ValueType, error) {
	value, err := n.getReferenceValue(scope.(*tick.Scope), executionState)
	if err != nil {
		return InvalidType, err
	}

	return valueTypeOf(reflect.TypeOf(value)), nil
}

func (n *EvalReferenceNode) EvalRegex(scope *tick.Scope, executionState ExecutionState) (*regexp.Regexp, error) {
	refValue, err := n.getReferenceValue(scope, executionState)
	if err != nil {
		return nil, err
	}

	if regexValue, isRegex := refValue.(*regexp.Regexp); isRegex {
		return regexValue, nil
	}

	return nil, ErrTypeGuardFailed{RequestedType: TRegex, ActualType: valueTypeOf(reflect.TypeOf(refValue))}
}

func (n *EvalReferenceNode) EvalString(scope *tick.Scope, executionState ExecutionState) (string, error) {
	refValue, err := n.getReferenceValue(scope, executionState)
	if err != nil {
		return "", err
	}

	if stringValue, isString := refValue.(string); isString {
		return stringValue, nil
	}

	return "", ErrTypeGuardFailed{RequestedType: TString, ActualType: valueTypeOf(reflect.TypeOf(refValue))}
}

func (n *EvalReferenceNode) EvalFloat(scope *tick.Scope, executionState ExecutionState) (float64, error) {
	refValue, err := n.getReferenceValue(scope, executionState)
	if err != nil {
		return float64(0), err
	}

	if float64Value, isFloat64 := refValue.(float64); isFloat64 {
		return float64Value, nil
	}

	return float64(0), ErrTypeGuardFailed{RequestedType: TFloat64, ActualType: valueTypeOf(reflect.TypeOf(refValue))}
}

func (n *EvalReferenceNode) EvalInt(scope *tick.Scope, executionState ExecutionState) (int64, error) {
	refValue, err := n.getReferenceValue(scope, executionState)
	if err != nil {
		return int64(0), err
	}

	if int64Value, isInt64 := refValue.(int64); isInt64 {
		return int64Value, nil
	}

	return int64(0), ErrTypeGuardFailed{RequestedType: TInt64, ActualType: valueTypeOf(reflect.TypeOf(refValue))}
}

func (n *EvalReferenceNode) EvalBool(scope *tick.Scope, executionState ExecutionState) (bool, error) {
	refValue, err := n.getReferenceValue(scope, executionState)
	if err != nil {
		return false, err
	}

	if boolValue, isBool := refValue.(bool); isBool {
		return boolValue, nil
	}

	return false, ErrTypeGuardFailed{RequestedType: TBool, ActualType: valueTypeOf(reflect.TypeOf(refValue))}
}
