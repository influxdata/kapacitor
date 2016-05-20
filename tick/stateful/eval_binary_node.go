package stateful

import (
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/influxdata/kapacitor/tick/ast"
)

type resultContainer struct {
	// IsBoolValue is kinda reundandant, but we want to be consistent with
	// the numeric fields
	IsBoolValue bool
	BoolValue   bool

	IsInt64Value bool
	Int64Value   int64

	IsFloat64Value bool
	Float64Value   float64

	IsStringValue bool
	StringValue   string

	IsDurationValue bool
	DurationValue   time.Duration
}

// this function shouldn't be used! only for throwing details error messages!
func (rc resultContainer) value() interface{} {
	switch {
	case rc.IsBoolValue:
		return rc.BoolValue

	case rc.IsInt64Value:
		return rc.Int64Value

	case rc.IsFloat64Value:
		return rc.Float64Value
	}

	return nil
}

// ErrSide wraps the error in the evaluation, we use this error to indicate the origin of the error
// left side or right side
type ErrSide struct {
	error
	IsLeft  bool
	IsRight bool
}

// Evaluation functions
type evaluationFn func(scope *Scope, executionState ExecutionState, left, right NodeEvaluator) (resultContainer, *ErrSide)

// EvalBinaryNode is stateful expression which
//  is evaluated using "expression trees" instead of stack based interpreter
type EvalBinaryNode struct {
	operator ast.TokenType

	// This the evaluation function for this comparison node
	// this function can be:
	//  *) specialized function
	//  *) dynamic function - which will delegate to specialized function
	evaluationFn evaluationFn

	// Saving a cache version NodeEvaluator so we don't need to cast
	// in every EvalBool call
	leftEvaluator NodeEvaluator
	leftType      ast.ValueType

	rightEvaluator NodeEvaluator
	rightType      ast.ValueType

	// Constant return type
	// If InvalidType then this node is dynamic.
	constReturnType ast.ValueType
}

func NewEvalBinaryNode(node *ast.BinaryNode) (*EvalBinaryNode, error) {
	if !ast.IsExprOperator(node.Operator) {
		return nil, fmt.Errorf("unknown binary operator %v", node.Operator)
	}
	b := &EvalBinaryNode{
		operator:        node.Operator,
		constReturnType: getConstantNodeType(node),
	}

	leftSideEvaluator, err := createNodeEvaluator(node.Left)
	if err != nil {
		return nil, fmt.Errorf("Failed to handle left node: %v", err)
	}

	rightSideEvaluator, err := createNodeEvaluator(node.Right)
	if err != nil {
		return nil, fmt.Errorf("Failed to handle right node: %v", err)
	}

	b.leftEvaluator = leftSideEvaluator

	b.rightEvaluator = rightSideEvaluator

	if b.leftEvaluator.IsDynamic() || b.rightEvaluator.IsDynamic() {
		b.evaluationFn = b.evaluateDynamicNode
	} else {
		b.leftType = getConstantNodeType(node.Left)
		b.rightType = getConstantNodeType(node.Right)

		b.evaluationFn = b.lookupEvaluationFn()
		if b.evaluationFn == nil {
			return nil, b.determineError(nil, ExecutionState{})
		}
	}

	return b, nil
}

func (n *EvalBinaryNode) Type(scope ReadOnlyScope, executionState ExecutionState) (ast.ValueType, error) {
	if n.constReturnType == ast.InvalidType {
		var err error
		// We are dynamic and we need to figure out our type
		n.leftType, err = n.leftEvaluator.Type(scope, executionState)
		if err != nil {
			return ast.InvalidType, err
		}
		n.rightType, err = n.rightEvaluator.Type(scope, executionState)
		if err != nil {
			return ast.InvalidType, err
		}
		// Do NOT cache this result in n.returnType since it can change.
		typ := binaryConstantTypes[operationKey{operator: n.operator, leftType: n.leftType, rightType: n.rightType}]
		if typ == ast.InvalidType {
			return typ, n.determineError(nil, ExecutionState{})
		}
		return typ, nil
	}
	return n.constReturnType, nil
}

func (n *EvalBinaryNode) IsDynamic() bool {
	if n.constReturnType != ast.InvalidType {
		return false
	}
	return n.leftEvaluator.IsDynamic() || n.rightEvaluator.IsDynamic()
}

func (n *EvalBinaryNode) EvalRegex(scope *Scope, executionState ExecutionState) (*regexp.Regexp, error) {
	return nil, ErrTypeGuardFailed{RequestedType: ast.TRegex, ActualType: n.constReturnType}
}

func (n *EvalBinaryNode) EvalTime(scope *Scope, executionState ExecutionState) (time.Time, error) {
	return time.Time{}, ErrTypeGuardFailed{RequestedType: ast.TTime, ActualType: n.constReturnType}
}

func (e *EvalBinaryNode) EvalDuration(scope *Scope, executionState ExecutionState) (time.Duration, error) {
	result, err := e.eval(scope, executionState)
	if err != nil {
		return 0, err.error
	}

	if result.IsDurationValue {
		return result.DurationValue, nil
	}

	return 0, fmt.Errorf("expression returned unexpected type %T", result.value())
}

func (e *EvalBinaryNode) EvalString(scope *Scope, executionState ExecutionState) (string, error) {
	result, err := e.eval(scope, executionState)
	if err != nil {
		return "", err.error
	}

	if result.IsStringValue {
		return result.StringValue, nil
	}

	return "", fmt.Errorf("expression returned unexpected type %T", result.value())
}

// EvalBool executes the expression based on eval bool
func (e *EvalBinaryNode) EvalBool(scope *Scope, executionState ExecutionState) (bool, error) {
	result, err := e.eval(scope, executionState)
	if err != nil {
		return false, err.error
	}

	if result.IsBoolValue {
		return result.BoolValue, nil
	}

	return false, fmt.Errorf("expression returned unexpected type %T", result.value())
}

// EvalNum executes the expression based on eval numeric
func (e *EvalBinaryNode) EvalFloat(scope *Scope, executionState ExecutionState) (float64, error) {
	result, err := e.eval(scope, executionState)
	if err != nil {
		return float64(0), err.error
	}

	if result.IsFloat64Value {
		return result.Float64Value, nil
	}

	if result.IsInt64Value {
		return float64(0), ErrTypeGuardFailed{RequestedType: ast.TFloat, ActualType: ast.TInt}
	}

	return float64(0), ErrTypeGuardFailed{RequestedType: ast.TFloat, ActualType: e.constReturnType}
}

func (e *EvalBinaryNode) EvalInt(scope *Scope, executionState ExecutionState) (int64, error) {
	result, err := e.eval(scope, executionState)
	if err != nil {
		return int64(0), err.error
	}

	if result.IsInt64Value {
		return result.Int64Value, nil
	}

	if result.IsFloat64Value {
		return int64(0), ErrTypeGuardFailed{RequestedType: ast.TInt, ActualType: ast.TFloat}
	}

	return int64(0), ErrTypeGuardFailed{RequestedType: ast.TInt, ActualType: e.constReturnType}

}

func (e *EvalBinaryNode) eval(scope *Scope, executionState ExecutionState) (resultContainer, *ErrSide) {
	if e.evaluationFn == nil {
		err := e.determineError(scope, executionState)
		return boolFalseResultContainer, &ErrSide{error: err}
	}

	evaluationResult, err := e.evaluationFn(scope, executionState, e.leftEvaluator, e.rightEvaluator)

	// This case can in dynamic nodes,
	// for example: RefNode("value") > NumberNode("float64")
	// in the first evaluation "value" is float64 so we will have float64 > float64 comparison fn
	// after the first evaluation, let's assume that "value" is changed to int64 - we need to change
	// the comparison fn
	if err != nil {
		if typeGuardErr, isTypeGuardError := err.error.(ErrTypeGuardFailed); isTypeGuardError {
			// Fix the type info, thanks to the type guard info
			if err.IsLeft {
				e.leftType = typeGuardErr.ActualType
			}

			if err.IsRight {
				e.rightType = typeGuardErr.ActualType
			}

			// redefine the evaluation fn
			e.evaluationFn = e.lookupEvaluationFn()
			if e.evaluationFn == nil {
				return boolFalseResultContainer, err
			}

			// try again
			return e.eval(scope, executionState)
		}
	}

	return evaluationResult, err
}

// evaluateDynamicNode fetches the value of the right and left node at evaluation time (aka "runtime")
// and find the matching evaluation function for the givne types - this is where the "specialisation" happens.
func (e *EvalBinaryNode) evaluateDynamicNode(scope *Scope, executionState ExecutionState, left, right NodeEvaluator) (resultContainer, *ErrSide) {
	var leftType ast.ValueType
	var rightType ast.ValueType
	var err error

	// For getting the type we must pass new execution state, since the node can be stateful (like function call)
	// and on the second in the specialiszation we might loose the correct state
	// For example: "count() == 1"
	//  1. we evaluate the left side and counter is 1 (upper ^ in this function)
	//  2. we evaluate the second time in "EvalBool"
	typeExecutionState := CreateExecutionState()

	if leftType, err = left.Type(scope, typeExecutionState); err != nil {
		return emptyResultContainer, &ErrSide{error: err, IsLeft: true}
	}

	if rightType, err = right.Type(scope, typeExecutionState); err != nil {
		return emptyResultContainer, &ErrSide{error: err, IsRight: true}
	}

	e.leftType = leftType
	e.rightType = rightType

	e.evaluationFn = e.lookupEvaluationFn()

	return e.eval(scope, executionState)
}

// Return an understandable error which is most specific to the issue.
func (e *EvalBinaryNode) determineError(scope *Scope, executionState ExecutionState) error {
	if scope != nil {
		typeExecutionState := CreateExecutionState()
		leftType, err := e.leftEvaluator.Type(scope, typeExecutionState)
		if err != nil {
			return fmt.Errorf("can't get the type of the left node: %s", err)
		}
		e.leftType = leftType

		if leftType == ast.InvalidType {
			return errors.New("left value is invalid value type")
		}

		rightType, err := e.rightEvaluator.Type(scope, typeExecutionState)
		if err != nil {
			return fmt.Errorf("can't get the type of the right node: %s", err)
		}
		e.rightType = rightType

		if rightType == ast.InvalidType {
			return errors.New("right value is invalid value type")
		}
	}

	if e.leftType != ast.InvalidType && !typeToBinaryOperators[e.leftType][e.operator] {
		return fmt.Errorf("invalid %s operator %v for type %s", operatorKind(e.operator), e.operator, e.leftType)
	} else if e.rightType != ast.InvalidType && !typeToBinaryOperators[e.leftType][e.operator] {
		return fmt.Errorf("invalid %s operator %v for type %s", operatorKind(e.operator), e.operator, e.rightType)
	}

	return fmt.Errorf("mismatched type to binary operator. got %s %v %s. see bool(), int(), float(), string(), duration()", e.leftType, e.operator, e.rightType)
}

func (e *EvalBinaryNode) lookupEvaluationFn() evaluationFn {
	info := evaluationFuncs[operationKey{operator: e.operator, leftType: e.leftType, rightType: e.rightType}]
	if info == nil {
		return nil
	}
	return info.f
}

var typeToBinaryOperators = (func() map[ast.ValueType]map[ast.TokenType]bool {
	// This map is built at "runtime" because we don't to have tight coupling
	// every time we had new "comparison operator" / "math operator" to update this map
	// and the performance cost is neglibile for doing so.

	result := make(map[ast.ValueType]map[ast.TokenType]bool)

	for opKey := range evaluationFuncs {
		// Left
		typeSet, exists := result[opKey.leftType]

		if !exists {
			result[opKey.leftType] = make(map[ast.TokenType]bool, 0)
			typeSet = result[opKey.leftType]
		}

		typeSet[opKey.operator] = true

		// Right
		typeSet, exists = result[opKey.rightType]

		if !exists {
			result[opKey.rightType] = make(map[ast.TokenType]bool, 0)
			typeSet = result[opKey.rightType]
		}

		typeSet[opKey.operator] = true
	}

	return result
})()

func operatorKind(operator ast.TokenType) string {
	switch {
	case ast.IsMathOperator(operator):
		return "math"
	case ast.IsCompOperator(operator):
		return "comparison"
	case ast.IsLogicalOperator(operator):
		return "logical"
	}

	// Actually, we shouldn't get here.. because this function is called only
	// after the operator validation!
	return "INVALID"
}
