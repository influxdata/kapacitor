package stateful

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/influxdata/kapacitor/tick"
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
	IsLeftSide  bool
	IsRightSide bool
}

// Evaluation functions
type evaluationFn func(scope *tick.Scope, executionState ExecutionState, left, right NodeEvaluator) (resultContainer, *ErrSide)

// EvalBinaryNode is stateful expression which
//  is evaluated using "expression trees" instead of stack based interpreter
type EvalBinaryNode struct {
	operator tick.TokenType

	// This the evaluation function for this comparison node
	// this function can be:
	//  *) specialized function
	//  *) dynamic function - which will delegate to specialized function
	evaluationFn evaluationFn

	// Saving a cache version NodeEvaluator so we don't need to cast
	// in every EvalBool call
	leftSideEvaluator NodeEvaluator
	leftSideType      ValueType

	rightSideEvaluator NodeEvaluator
	rightSideType      ValueType

	// Return type
	returnType ValueType
}

func NewEvalBinaryNode(node *tick.BinaryNode) (*EvalBinaryNode, error) {
	expression := &EvalBinaryNode{
		operator:   node.Operator,
		returnType: getConstantNodeType(node),
	}

	leftSideEvaluator, err := createNodeEvaluator(node.Left)
	if err != nil {
		return nil, fmt.Errorf("Failed to handle left node: %v", err)
	}

	rightSideEvaluator, err := createNodeEvaluator(node.Right)
	if err != nil {
		return nil, fmt.Errorf("Failed to handle right node: %v", err)
	}

	expression.leftSideEvaluator = leftSideEvaluator
	expression.rightSideEvaluator = rightSideEvaluator

	if isDynamicNode(node.Left) || isDynamicNode(node.Right) {
		expression.evaluationFn = expression.evaluateDynamicNode
	} else {
		expression.leftSideType = getConstantNodeType(node.Left)
		expression.rightSideType = getConstantNodeType(node.Right)

		expression.evaluationFn = expression.findEvaluationFn(expression.leftSideType, expression.rightSideType)
	}

	return expression, nil
}

func (n *EvalBinaryNode) Type(scope ReadOnlyScope, executionState ExecutionState) (ValueType, error) {
	return findNodeTypes(n.returnType, []NodeEvaluator{n.leftSideEvaluator, n.rightSideEvaluator}, scope, executionState)
}

func (n *EvalBinaryNode) EvalRegex(scope *tick.Scope, executionState ExecutionState) (*regexp.Regexp, error) {
	return nil, ErrTypeGuardFailed{RequestedType: TRegex, ActualType: n.returnType}
}

func (e *EvalBinaryNode) EvalString(scope *tick.Scope, executionState ExecutionState) (string, error) {
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
func (e *EvalBinaryNode) EvalBool(scope *tick.Scope, executionState ExecutionState) (bool, error) {
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
func (e *EvalBinaryNode) EvalFloat(scope *tick.Scope, executionState ExecutionState) (float64, error) {
	result, err := e.eval(scope, executionState)
	if err != nil {
		return float64(0), err.error
	}

	if result.IsFloat64Value {
		return result.Float64Value, nil
	}

	if result.IsInt64Value {
		return float64(0), ErrTypeGuardFailed{RequestedType: TFloat64, ActualType: TInt64}
	}

	return float64(0), ErrTypeGuardFailed{RequestedType: TFloat64, ActualType: e.returnType}
}

func (e *EvalBinaryNode) EvalInt(scope *tick.Scope, executionState ExecutionState) (int64, error) {
	result, err := e.eval(scope, executionState)
	if err != nil {
		return int64(0), err.error
	}

	if result.IsInt64Value {
		return result.Int64Value, nil
	}

	if result.IsFloat64Value {
		return int64(0), ErrTypeGuardFailed{RequestedType: TInt64, ActualType: TFloat64}
	}

	return int64(0), ErrTypeGuardFailed{RequestedType: TInt64, ActualType: e.returnType}

}

func (e *EvalBinaryNode) eval(scope *tick.Scope, executionState ExecutionState) (resultContainer, *ErrSide) {
	if e.evaluationFn == nil {
		err := e.determineError(scope, executionState, e.operator, e.leftSideEvaluator, e.rightSideEvaluator)
		return boolFalseResultContainer, &ErrSide{error: err}
	}

	evaluationResult, err := e.evaluationFn(scope, executionState, e.leftSideEvaluator, e.rightSideEvaluator)

	// This case can in dynamic nodes,
	// for example: RefNode("value") > NumberNode("float64")
	// in the first evaluation "value" is float64 so we will have float64 > float64 comparison fn
	// after the first evaluation, let's assume that "value" is changed to int64 - we need to change
	// the comparison fn
	if err != nil {
		if typeGuardErr, isTypeGuardError := err.error.(ErrTypeGuardFailed); isTypeGuardError {
			// Fix the type info, thanks to the type guard info
			if err.IsLeftSide {
				e.leftSideType = typeGuardErr.ActualType
			}

			if err.IsRightSide {
				e.rightSideType = typeGuardErr.ActualType
			}

			// re-find the evaluation fn
			e.evaluationFn = e.findEvaluationFn(e.leftSideType, e.rightSideType)

			// recurse (so we handle nil evaluationFn, and etc)
			return e.eval(scope, executionState)
		}
	}

	return evaluationResult, err
}

// evaluateDynamicNode fetches the value of the right and left node at evaluation time (aka "runtime")
// and find the matching evaluation function for the givne types - this is where the "specialisation" happens.
func (e *EvalBinaryNode) evaluateDynamicNode(scope *tick.Scope, executionState ExecutionState, left, right NodeEvaluator) (resultContainer, *ErrSide) {
	var leftSideType ValueType
	var rightSideType ValueType
	var err error

	// For getting the type we must pass new execution state, since the node can be stateful (like function call)
	// and on the second in the specialiszation we might loose the correct state
	// For example: "count() == 1"
	//  1. we evaluate the left side and counter is 1 (upper ^ in this function)
	//  2. we evaluate the second time in "EvalBool"
	typeExecutionState := CreateExecutionState()

	if leftSideType, err = left.Type(scope, typeExecutionState); err != nil {
		return emptyResultContainer, &ErrSide{error: err, IsLeftSide: true}
	}

	if rightSideType, err = right.Type(scope, typeExecutionState); err != nil {
		return emptyResultContainer, &ErrSide{error: err, IsRightSide: true}
	}

	e.leftSideType = leftSideType
	e.rightSideType = rightSideType

	e.evaluationFn = e.findEvaluationFn(e.leftSideType, e.rightSideType)

	return e.eval(scope, executionState)
}

func (e *EvalBinaryNode) determineError(scope *tick.Scope, executionState ExecutionState, operator tick.TokenType, left, right NodeEvaluator) error {
	// Validate the evaluation parameters:
	//  *) not support types like arrays
	//  *) not comparison operator
	//  *) invalid operator for the given type

	typeExecutionState := CreateExecutionState()

	leftValueType, err := left.Type(scope, typeExecutionState)
	if err != nil {
		return fmt.Errorf("Can't get the type of the left node: %s", err)
	}
	leftTypeName := leftValueType.String()

	if leftValueType == InvalidType {
		return errors.New("left value is invalid value type")
	}

	rightValueType, err := right.Type(scope, typeExecutionState)
	if err != nil {
		return fmt.Errorf("Can't get the type of the right node: %s", err)
	}
	rightTypeName := rightValueType.String()

	if rightValueType == InvalidType {
		return errors.New("right value is invalid value type")
	}

	err = isValidOperator(e.operator, leftValueType, rightValueType)
	if err != nil {
		return err
	}

	return fmt.Errorf("mismatched type to binary operator. got %s %v %s. see bool(), int(), float()", leftTypeName, e.operator, rightTypeName)
}

func (e *EvalBinaryNode) findEvaluationFn(leftType, rightType ValueType) evaluationFn {
	return evaluationFuncs[operationKey{operator: e.operator, leftType: leftType, rightType: rightType}]
}

// Type to comparison operator - for comparison operator validation (see: isValidBinaryOperator)
// The key is value type where the value is set of TokenType
var typeToBinaryOperators = (func() map[ValueType]map[tick.TokenType]bool {
	// This map is built at "runtime" because we don't to have tight coupling
	// every time we had new "comparison operator" / "math operator" to update this map
	// and the performance cost is neglibile for doing so.

	result := make(map[ValueType]map[tick.TokenType]bool, 0)

	for opKey := range evaluationFuncs {
		typeSet, exists := result[opKey.leftType]

		if !exists {
			result[opKey.leftType] = make(map[tick.TokenType]bool, 0)
			typeSet = result[opKey.leftType]
		}

		typeSet[opKey.operator] = true
		result[opKey.leftType] = typeSet
	}

	return result
})()

// isValidOperator returns whether the operator and left/right nodes are valid for comparison, if not
// false will be returned with correct error message
func isValidOperator(operator tick.TokenType, leftNodeType, rightNodeType ValueType) error {
	if !tick.IsExprOperator(operator) {
		return fmt.Errorf("return: unknown operator %v", operator)
	}

	var nodeType ValueType

	// Only for TRegex we determine the validity of the operator by the right node
	if rightNodeType == TRegex {
		nodeType = rightNodeType
	} else {
		nodeType = leftNodeType
	}

	isValid := typeToBinaryOperators[nodeType][operator]
	if !isValid {
		return fmt.Errorf("invalid %s %s operator %v", nodeType.String(), operatorType(operator), operator)
	}

	return nil
}

func operatorType(operator tick.TokenType) string {
	if tick.IsMathOperator(operator) {
		return "math"
	}

	if tick.IsCompOperator(operator) {
		return "comparison"
	}

	// Actually, we shouldn't get here.. because this function is called only
	// after the operator validation!
	return "INVALID"
}
