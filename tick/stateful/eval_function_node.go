package stateful

import (
	"fmt"
	"reflect"
	"regexp"

	"github.com/influxdata/kapacitor/tick"
)

type EvalFunctionNode struct {
	funcName       string
	argsEvaluators []NodeEvaluator
}

func NewEvalFunctionNode(funcNode *tick.FunctionNode) (*EvalFunctionNode, error) {
	evalFuncNode := &EvalFunctionNode{
		funcName: funcNode.Func,
	}

	evalFuncNode.argsEvaluators = make([]NodeEvaluator, 0, len(funcNode.Args))
	for i, argNode := range funcNode.Args {
		argEvaluator, err := createNodeEvaluator(argNode)
		if err != nil {
			return nil, fmt.Errorf("Failed to handle %v argument: %v", i+1, err)
		}

		evalFuncNode.argsEvaluators = append(evalFuncNode.argsEvaluators, argEvaluator)
	}

	return evalFuncNode, nil
}

func (n *EvalFunctionNode) Type(scope ReadOnlyScope, executionState ExecutionState) (ValueType, error) {
	// PERF: today we are evaluating the function, it will be much faster if will type info the function it self
	result, err := n.callFunction(scope.(*tick.Scope), executionState)
	if err != nil {
		return InvalidType, err
	}

	// We can't cache here the result (although it's very tempting ;))
	// because can't trust function to return always the same consistent type
	return valueTypeOf(reflect.TypeOf(result)), nil
}

// callFunction - core method for evaluating function where all NodeEvaluator methods should use
func (n *EvalFunctionNode) callFunction(scope *tick.Scope, executionState ExecutionState) (interface{}, error) {
	args := make([]interface{}, 0, len(n.argsEvaluators))
	for i, argEvaluator := range n.argsEvaluators {
		value, err := eval(argEvaluator, scope, executionState)
		if err != nil {
			return nil, fmt.Errorf("Failed to handle %v argument: %v", i+1, err)
		}

		args = append(args, value)
	}

	f := executionState.Funcs[n.funcName]

	if f == nil {
		return nil, fmt.Errorf("undefined function: %q", n.funcName)
	}

	ret, err := f.Call(args...)
	if err != nil {
		return nil, fmt.Errorf("error calling %q: %s", n.funcName, err)
	}

	return ret, nil
}

func (n *EvalFunctionNode) EvalRegex(scope *tick.Scope, executionState ExecutionState) (*regexp.Regexp, error) {
	refValue, err := n.callFunction(scope, executionState)
	if err != nil {
		return nil, err
	}

	if regexValue, isRegex := refValue.(*regexp.Regexp); isRegex {
		return regexValue, nil
	}

	return nil, ErrTypeGuardFailed{RequestedType: TRegex, ActualType: valueTypeOf(reflect.TypeOf(refValue))}
}

func (n *EvalFunctionNode) EvalString(scope *tick.Scope, executionState ExecutionState) (string, error) {
	refValue, err := n.callFunction(scope, executionState)
	if err != nil {
		return "", err
	}

	if stringValue, isString := refValue.(string); isString {
		return stringValue, nil
	}

	return "", ErrTypeGuardFailed{RequestedType: TString, ActualType: valueTypeOf(reflect.TypeOf(refValue))}
}

func (n *EvalFunctionNode) EvalFloat(scope *tick.Scope, executionState ExecutionState) (float64, error) {
	refValue, err := n.callFunction(scope, executionState)
	if err != nil {
		return float64(0), err
	}

	if float64Value, isFloat64 := refValue.(float64); isFloat64 {
		return float64Value, nil
	}

	return float64(0), ErrTypeGuardFailed{RequestedType: TFloat64, ActualType: valueTypeOf(reflect.TypeOf(refValue))}
}

func (n *EvalFunctionNode) EvalInt(scope *tick.Scope, executionState ExecutionState) (int64, error) {
	refValue, err := n.callFunction(scope, executionState)
	if err != nil {
		return int64(0), err
	}

	if int64Value, isInt64 := refValue.(int64); isInt64 {
		return int64Value, nil
	}

	return int64(0), ErrTypeGuardFailed{RequestedType: TInt64, ActualType: valueTypeOf(reflect.TypeOf(refValue))}
}

func (n *EvalFunctionNode) EvalBool(scope *tick.Scope, executionState ExecutionState) (bool, error) {
	refValue, err := n.callFunction(scope, executionState)
	if err != nil {
		return false, err
	}

	if boolValue, isBool := refValue.(bool); isBool {
		return boolValue, nil
	}

	return false, ErrTypeGuardFailed{RequestedType: TBool, ActualType: valueTypeOf(reflect.TypeOf(refValue))}
}

// eval - generic evaluation until we have reflection/introspection capabillities so we can know the type of args
// and return type, we can remove this entirely
func eval(n NodeEvaluator, scope *tick.Scope, executionState ExecutionState) (interface{}, error) {
	retType, err := n.Type(scope, CreateExecutionState())
	if err != nil {
		return nil, err
	}

	switch retType {
	case TBool:
		return n.EvalBool(scope, executionState)
	case TInt64:
		return n.EvalInt(scope, executionState)
	case TFloat64:
		return n.EvalFloat(scope, executionState)
	case TString:
		return n.EvalString(scope, executionState)
	case TRegex:
		return n.EvalRegex(scope, executionState)
	default:
		return nil, fmt.Errorf("expression returned unexpected type %s", retType)
	}

}
