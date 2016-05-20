package stateful

import (
	"fmt"
	"regexp"
	"time"

	"github.com/influxdata/kapacitor/tick/ast"
)

type EvalFunctionNode struct {
	funcName       string
	argsEvaluators []NodeEvaluator
}

func NewEvalFunctionNode(funcNode *ast.FunctionNode) (*EvalFunctionNode, error) {
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

func (n *EvalFunctionNode) Type(scope ReadOnlyScope, executionState ExecutionState) (ast.ValueType, error) {
	// PERF: today we are evaluating the function, it will be much faster if will type info the function it self
	result, err := n.callFunction(scope.(*Scope), executionState)
	if err != nil {
		return ast.InvalidType, err
	}

	// We can't cache here the result (although it's very tempting ;))
	// because can't trust function to return always the same consistent type
	return ast.TypeOf(result), nil
}

func (n *EvalFunctionNode) IsDynamic() bool {
	return true
}

// callFunction - core method for evaluating function where all NodeEvaluator methods should use
func (n *EvalFunctionNode) callFunction(scope *Scope, executionState ExecutionState) (interface{}, error) {
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

func (n *EvalFunctionNode) EvalRegex(scope *Scope, executionState ExecutionState) (*regexp.Regexp, error) {
	refValue, err := n.callFunction(scope, executionState)
	if err != nil {
		return nil, err
	}

	if regexValue, isRegex := refValue.(*regexp.Regexp); isRegex {
		return regexValue, nil
	}

	return nil, ErrTypeGuardFailed{RequestedType: ast.TRegex, ActualType: ast.TypeOf(refValue)}
}

func (n *EvalFunctionNode) EvalTime(scope *Scope, executionState ExecutionState) (time.Time, error) {
	refValue, err := n.callFunction(scope, executionState)
	if err != nil {
		return time.Time{}, err
	}

	if timeValue, isTime := refValue.(time.Time); isTime {
		return timeValue, nil
	}

	return time.Time{}, ErrTypeGuardFailed{RequestedType: ast.TTime, ActualType: ast.TypeOf(refValue)}
}

func (n *EvalFunctionNode) EvalDuration(scope *Scope, executionState ExecutionState) (time.Duration, error) {
	refValue, err := n.callFunction(scope, executionState)
	if err != nil {
		return 0, err
	}

	if durValue, isDuration := refValue.(time.Duration); isDuration {
		return durValue, nil
	}

	return 0, ErrTypeGuardFailed{RequestedType: ast.TDuration, ActualType: ast.TypeOf(refValue)}
}

func (n *EvalFunctionNode) EvalString(scope *Scope, executionState ExecutionState) (string, error) {
	refValue, err := n.callFunction(scope, executionState)
	if err != nil {
		return "", err
	}

	if stringValue, isString := refValue.(string); isString {
		return stringValue, nil
	}

	return "", ErrTypeGuardFailed{RequestedType: ast.TString, ActualType: ast.TypeOf(refValue)}
}

func (n *EvalFunctionNode) EvalFloat(scope *Scope, executionState ExecutionState) (float64, error) {
	refValue, err := n.callFunction(scope, executionState)
	if err != nil {
		return float64(0), err
	}

	if float64Value, isFloat64 := refValue.(float64); isFloat64 {
		return float64Value, nil
	}

	return float64(0), ErrTypeGuardFailed{RequestedType: ast.TFloat, ActualType: ast.TypeOf(refValue)}
}

func (n *EvalFunctionNode) EvalInt(scope *Scope, executionState ExecutionState) (int64, error) {
	refValue, err := n.callFunction(scope, executionState)
	if err != nil {
		return int64(0), err
	}

	if int64Value, isInt64 := refValue.(int64); isInt64 {
		return int64Value, nil
	}

	return int64(0), ErrTypeGuardFailed{RequestedType: ast.TInt, ActualType: ast.TypeOf(refValue)}
}

func (n *EvalFunctionNode) EvalBool(scope *Scope, executionState ExecutionState) (bool, error) {
	refValue, err := n.callFunction(scope, executionState)
	if err != nil {
		return false, err
	}

	if boolValue, isBool := refValue.(bool); isBool {
		return boolValue, nil
	}

	return false, ErrTypeGuardFailed{RequestedType: ast.TBool, ActualType: ast.TypeOf(refValue)}
}

// eval - generic evaluation until we have reflection/introspection capabillities so we can know the type of args
// and return type, we can remove this entirely
func eval(n NodeEvaluator, scope *Scope, executionState ExecutionState) (interface{}, error) {
	retType, err := n.Type(scope, CreateExecutionState())
	if err != nil {
		return nil, err
	}

	switch retType {
	case ast.TBool:
		return n.EvalBool(scope, executionState)
	case ast.TInt:
		return n.EvalInt(scope, executionState)
	case ast.TFloat:
		return n.EvalFloat(scope, executionState)
	case ast.TString:
		return n.EvalString(scope, executionState)
	case ast.TRegex:
		return n.EvalRegex(scope, executionState)
	case ast.TTime:
		return n.EvalTime(scope, executionState)
	case ast.TDuration:
		return n.EvalDuration(scope, executionState)
	default:
		return nil, fmt.Errorf("function arg expression returned unexpected type %s", retType)
	}

}
