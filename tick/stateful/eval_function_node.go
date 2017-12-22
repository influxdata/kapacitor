package stateful

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/influxdata/kapacitor/tick/ast"
	"github.com/pkg/errors"
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

func (n *EvalFunctionNode) String() string {
	args := []string{}
	for _, argEvaluator := range n.argsEvaluators {
		args = append(args, fmt.Sprintf("%s", argEvaluator))
	}

	return n.funcName + "(" + strings.Join(args, ", ") + ")"
}

func (n *EvalFunctionNode) Type(scope ReadOnlyScope) (ast.ValueType, error) {
	f := lookupFunc(n.funcName, builtinFuncs, scope)
	if f == nil {
		return ast.InvalidType, fmt.Errorf("undefined function: %q", n.funcName)
	}
	signature := f.Signature()

	domain := Domain{}
	for i, argEvaluator := range n.argsEvaluators {
		t, err := argEvaluator.Type(scope)
		if err != nil {
			return ast.InvalidType, fmt.Errorf("Failed to handle %v argument: %v", i+1, err)
		}
		domain[i] = t
	}

	if gotLen, expLen := len(n.argsEvaluators), len(domain); gotLen > expLen {
		err := ErrWrongFuncSignature{Name: n.funcName, DomainProvided: domain, Func: f}
		return ast.InvalidType, errors.Wrapf(err, "too many arguments provided")
	}

	retType, ok := signature[domain]
	if !ok {
		args := []string{}
		missing := []string{}
		for i, argEvaluator := range n.argsEvaluators {
			t, err := argEvaluator.Type(scope)
			if err != nil {
				// This should never happen
				return ast.InvalidType, fmt.Errorf("Failed to handle %v argument: %v", i+1, err)
			}

			if t == ast.TMissing {
				missing = append(missing, fmt.Sprintf("%s", argEvaluator))
			}

			args = append(args, fmt.Sprintf("%s", argEvaluator))
		}

		if len(missing) > 0 {
			return ast.InvalidType, ErrMissingType{Name: n.funcName, Args: missing, Scope: scope.References()}
		}

		return ast.InvalidType, ErrWrongFuncSignature{Name: n.funcName, ArgLiterals: args, DomainProvided: domain, Func: f}
	}

	return retType, nil
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

	f := lookupFunc(n.funcName, executionState.Funcs, scope)
	if f == nil {
		return ast.InvalidType, fmt.Errorf("undefined function: %q", n.funcName)
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

func (n *EvalFunctionNode) EvalMissing(scope *Scope, executionState ExecutionState) (*ast.Missing, error) {
	refValue, err := n.callFunction(scope, executionState)
	if err != nil {
		return nil, err
	}

	if missingValue, isMissing := refValue.(*ast.Missing); isMissing {
		return missingValue, nil
	}
	return nil, ErrTypeGuardFailed{RequestedType: ast.TMissing, ActualType: ast.TypeOf(refValue)}
}

// eval - generic evaluation until we have reflection/introspection capabillities so we can know the type of args
// and return type, we can remove this entirely
func eval(n NodeEvaluator, scope *Scope, executionState ExecutionState) (interface{}, error) {
	retType, err := n.Type(scope)
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
	case ast.TMissing:
		v, err := n.EvalMissing(scope, executionState)
		if err != nil && !strings.Contains(err.Error(), "missing value") {
			return v, err
		}
		return v, nil
	default:
		return nil, fmt.Errorf("function arg expression returned unexpected type %s", retType)
	}

}

func lookupFunc(name string, funcs Funcs, scope ReadOnlyScope) Func {
	f := funcs[name]
	if f != nil {
		return f
	}

	df := scope.DynamicFunc(name)
	if df != nil {
		return df
	}

	// Return nil here explicitly so its a nil Func not nil *DynamicFunc
	// returning scope.DynamicFunc(name) caused nil check in callFunction
	// f == nil to evaluate to false even though f was nil. This was weird
	// enough that I felt it warranted a comment.
	return nil
}
