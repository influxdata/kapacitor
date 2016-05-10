package stateful_test

import (
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/kapacitor/tick"
	"github.com/influxdata/kapacitor/tick/stateful"
)

type keyStruct struct {
	lhs interface{}
	rhs interface{}
	op  tick.TokenType
}

func TestExpression_EvalNum_KeepsFunctionsState(t *testing.T) {
	se := mustCompileExpression(t, &tick.FunctionNode{
		Func: "sigma",
		Args: []tick.Node{&tick.ReferenceNode{Reference: "value"}},
	})

	// first
	scope := tick.NewScope()
	scope.Set("value", float64(97.1))
	result, err := se.EvalNum(scope)
	if err != nil {
		t.Errorf("First: Got unexpected error: %v", err)
	}

	if result != float64(0) {
		t.Errorf("First: expected count to be math.NaN() but got %v", result)
	}

	// second
	scope.Set("value", float64(92.6))
	result, err = se.EvalNum(scope)
	if err != nil {
		t.Errorf("Second: Got unexpected error: %v", err)
	}

	if result != float64(0.7071067811865476) {
		t.Errorf("Second: expected count to be float64(0.7071067811865476)  but got %v", result)
	}

}

func TestExpression_EvalBool_BinaryNodeWithDurationNode(t *testing.T) {
	se, err := stateful.NewExpression(&tick.BinaryNode{
		Operator: tick.TokenGreater,
		Left: &tick.DurationNode{
			Dur: time.Minute,
		},
		Right: &tick.DurationNode{
			Dur: time.Second,
		},
	})

	expectedError := errors.New("Failed to handle left node: Given node type is not valid evaluation node: *tick.DurationNode")

	if err == nil {
		t.Errorf("Expected error, but got expression: %v", se)
	}

	if err != nil && err.Error() != expectedError.Error() {
		t.Errorf("Got unexpected error:\nexpected: %v\ngot: %v", expectedError, err)
	}

}

func TestExpression_Eval_NotSupportedNode(t *testing.T) {
	// Passing IdentifierNode, yeah.. this crazy test, but we want to make sure
	// we don't have panics or crashes
	se, err := stateful.NewExpression(&tick.IdentifierNode{})
	expectedError := errors.New("Given node type is not valid evaluation node: *tick.IdentifierNode")
	if err == nil {
		t.Errorf("EvalBool: Expected error, but got expression: %v", se)
	}

	if err != nil && err.Error() != expectedError.Error() {
		t.Errorf("EvalBool: Got unexpected error:\nexpected: %v\ngot: %v", expectedError, err)
	}

	// BinaryNode - Left is identifier
	se, err = stateful.NewExpression(&tick.BinaryNode{
		Operator: tick.TokenEqual,
		Left:     &tick.IdentifierNode{},
		Right:    &tick.BoolNode{Bool: true},
	})

	expectedError = errors.New("Failed to handle left node: Given node type is not valid evaluation node: *tick.IdentifierNode")
	if err == nil {
		t.Errorf("EvalBool BinaryNode(Left=>Identifier): Expected error, but got expression: %v", se)
	}

	if err != nil && err.Error() != expectedError.Error() {
		t.Errorf("EvalBool BinaryNode(Left=>Identifier): Got unexpected error:\nexpected: %v\ngot: %v", expectedError, err)
	}

	// BinaryNode - Right is identifier
	se, err = stateful.NewExpression(&tick.BinaryNode{
		Operator: tick.TokenEqual,
		Left:     &tick.BoolNode{Bool: true},
		Right:    &tick.IdentifierNode{},
	})

	expectedError = errors.New("Failed to handle right node: Given node type is not valid evaluation node: *tick.IdentifierNode")
	if err == nil {
		t.Errorf("EvalBool BinaryNode(Right=>Identifier): Expected error, but got expression: %v", se)
	}

	if err != nil && err.Error() != expectedError.Error() {
		t.Errorf("EvalBool BinaryNode(Right=>Identifier): Got unexpected error:\nexpected: %v\ngot: %v", expectedError, err)
	}
}

func TestExpression_Eval_NodeAndEvalTypeNotMatching(t *testing.T) {
	// Test EvalBool against BinaryNode that returns math result
	se := mustCompileExpression(t, &tick.BinaryNode{
		Operator: tick.TokenPlus,
		Left: &tick.NumberNode{
			IsFloat: true,
			Float64: float64(5),
		},
		Right: &tick.NumberNode{
			IsFloat: true,
			Float64: float64(10),
		},
	})

	result, err := se.EvalBool(tick.NewScope())
	expectedError := errors.New("expression returned unexpected type float64")
	if err == nil {
		t.Errorf("EvalBool: Expected error result, but got result: %v", result)
	}

	if err != nil && err.Error() != expectedError.Error() {
		t.Errorf("EvalBool: Got unexpected error:\nexpected: %v\ngot: %v", expectedError, err)
	}

	// Test EvalNum against BinaryNode that returns bool result
	se = mustCompileExpression(t, &tick.BinaryNode{
		Operator: tick.TokenOr,
		Left: &tick.BoolNode{
			Bool: true,
		},
		Right: &tick.BoolNode{
			Bool: false,
		},
	})

	numResult, err := se.EvalNum(tick.NewScope())
	expectedError = errors.New("expression returned unexpected type boolean")
	if err == nil {
		t.Errorf("EvalNum: Expected error result, but got result: %v", numResult)
	}

	if err != nil && err.Error() != expectedError.Error() {
		t.Errorf("EvalNum: Got unexpected error:\nexpected: %v\ngot: %v", expectedError, err)
	}
}

func TestExpression_EvalBool_BoolNode(t *testing.T) {
	leftValues := []interface{}{true, false}

	// Right values are the same as left, just add a mismatch case
	rightValues := []interface{}{true, false, "NON_BOOL_VALUE"}
	operators := []tick.TokenType{tick.TokenEqual, tick.TokenNotEqual, tick.TokenAnd, tick.TokenOr, tick.TokenLess}

	createBoolNode := func(v interface{}) tick.Node {
		if strValue, isString := v.(string); isString {
			return &tick.StringNode{
				Literal: strValue,
			}
		}

		return &tick.BoolNode{
			Bool: v.(bool),
		}
	}

	runCompiledEvalBoolTests(t, createBoolNode, leftValues, rightValues, operators, map[keyStruct]interface{}{
		// Left: True, Right: True
		keyStruct{true, true, tick.TokenEqual}:    true,
		keyStruct{true, true, tick.TokenNotEqual}: false,
		keyStruct{true, true, tick.TokenAnd}:      true,
		keyStruct{true, true, tick.TokenOr}:       true,

		// Left: True, Right: False
		keyStruct{true, false, tick.TokenEqual}:    false,
		keyStruct{true, false, tick.TokenNotEqual}: true,
		keyStruct{true, false, tick.TokenAnd}:      false,
		keyStruct{true, false, tick.TokenOr}:       true,

		// Left: False, Right: True
		keyStruct{false, true, tick.TokenEqual}:    false,
		keyStruct{false, true, tick.TokenNotEqual}: true,
		keyStruct{false, true, tick.TokenAnd}:      false,
		keyStruct{false, true, tick.TokenOr}:       true,

		// Left: False, Right: False
		keyStruct{false, false, tick.TokenEqual}:    true,
		keyStruct{false, false, tick.TokenNotEqual}: false,
		keyStruct{false, false, tick.TokenAnd}:      false,
		keyStruct{false, false, tick.TokenOr}:       false,
	}, map[keyStruct]error{
		// Check invalid bool operator
		keyStruct{true, true, tick.TokenLess}:   errors.New("invalid boolean comparison operator <"),
		keyStruct{true, false, tick.TokenLess}:  errors.New("invalid boolean comparison operator <"),
		keyStruct{false, true, tick.TokenLess}:  errors.New("invalid boolean comparison operator <"),
		keyStruct{false, false, tick.TokenLess}: errors.New("invalid boolean comparison operator <"),

		// (Redundant test case)
		keyStruct{true, "NON_BOOL_VALUE", tick.TokenLess}:  errors.New("invalid boolean comparison operator <"),
		keyStruct{true, "NON_BOOL_VALUE", tick.TokenLess}:  errors.New("invalid boolean comparison operator <"),
		keyStruct{false, "NON_BOOL_VALUE", tick.TokenLess}: errors.New("invalid boolean comparison operator <"),
		keyStruct{false, "NON_BOOL_VALUE", tick.TokenLess}: errors.New("invalid boolean comparison operator <"),

		// Left: True, Right: "NON_BOOL_VALUE"
		keyStruct{true, "NON_BOOL_VALUE", tick.TokenEqual}:    errors.New("mismatched type to binary operator. got boolean == string. see bool(), int(), float()"),
		keyStruct{true, "NON_BOOL_VALUE", tick.TokenNotEqual}: errors.New("mismatched type to binary operator. got boolean != string. see bool(), int(), float()"),
		keyStruct{true, "NON_BOOL_VALUE", tick.TokenAnd}:      errors.New("mismatched type to binary operator. got boolean AND string. see bool(), int(), float()"),
		keyStruct{true, "NON_BOOL_VALUE", tick.TokenOr}:       errors.New("mismatched type to binary operator. got boolean OR string. see bool(), int(), float()"),

		// Left: False, Right: "NON_BOOL_VALUE"
		keyStruct{false, "NON_BOOL_VALUE", tick.TokenEqual}:    errors.New("mismatched type to binary operator. got boolean == string. see bool(), int(), float()"),
		keyStruct{false, "NON_BOOL_VALUE", tick.TokenNotEqual}: errors.New("mismatched type to binary operator. got boolean != string. see bool(), int(), float()"),
		keyStruct{false, "NON_BOOL_VALUE", tick.TokenAnd}:      errors.New("mismatched type to binary operator. got boolean AND string. see bool(), int(), float()"),
		keyStruct{false, "NON_BOOL_VALUE", tick.TokenOr}:       errors.New("mismatched type to binary operator. got boolean OR string. see bool(), int(), float()"),

		// Left: "NON_BOOL_VALUE", Right: True
		keyStruct{"NON_BOOL_VALUE", true, tick.TokenEqual}:    errors.New("mismatched type to binary operator. got string == bool. see bool(), int(), float()"),
		keyStruct{"NON_BOOL_VALUE", true, tick.TokenNotEqual}: errors.New("mismatched type to binary operator. got string != bool. see bool(), int(), float()"),
		keyStruct{"NON_BOOL_VALUE", true, tick.TokenAnd}:      errors.New("mismatched type to binary operator. got string AND bool. see bool(), int(), float()"),
		keyStruct{"NON_BOOL_VALUE", true, tick.TokenOr}:       errors.New("mismatched type to binary operator. got string OR bool. see bool(), int(), float()"),

		// Left: "NON_BOOL_VALUE", Right: False
		keyStruct{"NON_BOOL_VALUE", false, tick.TokenEqual}:    errors.New("mismatched type to binary operator. got string == bool. see bool(), int(), float()"),
		keyStruct{"NON_BOOL_VALUE", false, tick.TokenNotEqual}: errors.New("mismatched type to binary operator. got string != bool. see bool(), int(), float()"),
		keyStruct{"NON_BOOL_VALUE", false, tick.TokenAnd}:      errors.New("mismatched type to binary operator. got string AND bool. see bool(), int(), float()"),
		keyStruct{"NON_BOOL_VALUE", false, tick.TokenOr}:       errors.New("mismatched type to binary operator. got string OR bool. see bool(), int(), float()"),
	})

}

func TestExpression_EvalBool_NumberNode(t *testing.T) {
	leftValues := []interface{}{float64(5), float64(10), int64(5)}
	rightValues := []interface{}{float64(5), float64(10), int64(5), "NON_INT_VALUE"}

	operators := []tick.TokenType{tick.TokenEqual, tick.TokenNotEqual, tick.TokenGreater, tick.TokenGreaterEqual, tick.TokenLessEqual, tick.TokenLess, tick.TokenOr}

	createNumberNode := func(v interface{}) tick.Node {
		switch value := v.(type) {
		case float64:
			return &tick.NumberNode{
				IsFloat: true,
				Float64: value,
			}
		case int64:
			return &tick.NumberNode{
				IsInt: true,
				Int64: value,
			}
		// For the error case
		case string:
			return &tick.StringNode{
				Literal: value,
			}
		default:
			t.Fatalf("value supplied to createNumberNode is not string/int64/float64: %t", v)
			return nil
		}
	}

	runCompiledEvalBoolTests(t, createNumberNode, leftValues, rightValues, operators, map[keyStruct]interface{}{
		// Left is float64(5), Right is float64(5)
		keyStruct{float64(5), float64(5), tick.TokenEqual}:        true,
		keyStruct{float64(5), float64(5), tick.TokenNotEqual}:     false,
		keyStruct{float64(5), float64(5), tick.TokenGreater}:      false,
		keyStruct{float64(5), float64(5), tick.TokenGreaterEqual}: true,
		keyStruct{float64(5), float64(5), tick.TokenLess}:         false,
		keyStruct{float64(5), float64(5), tick.TokenLessEqual}:    true,

		// Left is float64(5), Right is float64(10)
		keyStruct{float64(5), float64(10), tick.TokenEqual}:        false,
		keyStruct{float64(5), float64(10), tick.TokenNotEqual}:     true,
		keyStruct{float64(5), float64(10), tick.TokenGreater}:      false,
		keyStruct{float64(5), float64(10), tick.TokenGreaterEqual}: false,
		keyStruct{float64(5), float64(10), tick.TokenLess}:         true,
		keyStruct{float64(5), float64(10), tick.TokenLessEqual}:    true,

		// Left is float64(5), Right is int64(5)
		keyStruct{float64(5), int64(5), tick.TokenEqual}:        true,
		keyStruct{float64(5), int64(5), tick.TokenNotEqual}:     false,
		keyStruct{float64(5), int64(5), tick.TokenGreater}:      false,
		keyStruct{float64(5), int64(5), tick.TokenGreaterEqual}: true,
		keyStruct{float64(5), int64(5), tick.TokenLess}:         false,
		keyStruct{float64(5), int64(5), tick.TokenLessEqual}:    true,

		// Left is float64(10), Right is float64(5)
		keyStruct{float64(10), float64(5), tick.TokenEqual}:        false,
		keyStruct{float64(10), float64(5), tick.TokenNotEqual}:     true,
		keyStruct{float64(10), float64(5), tick.TokenGreater}:      true,
		keyStruct{float64(10), float64(5), tick.TokenGreaterEqual}: true,
		keyStruct{float64(10), float64(5), tick.TokenLess}:         false,
		keyStruct{float64(10), float64(5), tick.TokenLessEqual}:    false,

		// Left is float64(10), Right is float64(10)
		keyStruct{float64(10), float64(10), tick.TokenEqual}:        true,
		keyStruct{float64(10), float64(10), tick.TokenNotEqual}:     false,
		keyStruct{float64(10), float64(10), tick.TokenGreater}:      false,
		keyStruct{float64(10), float64(10), tick.TokenGreaterEqual}: true,
		keyStruct{float64(10), float64(10), tick.TokenLess}:         false,
		keyStruct{float64(10), float64(10), tick.TokenLessEqual}:    true,

		// Left is float64(10), Right is float64(5)
		keyStruct{float64(10), int64(5), tick.TokenEqual}:        false,
		keyStruct{float64(10), int64(5), tick.TokenNotEqual}:     true,
		keyStruct{float64(10), int64(5), tick.TokenGreater}:      true,
		keyStruct{float64(10), int64(5), tick.TokenGreaterEqual}: true,
		keyStruct{float64(10), int64(5), tick.TokenLess}:         false,
		keyStruct{float64(10), int64(5), tick.TokenLessEqual}:    false,

		// Left is int64(10), Right is float64(5)
		keyStruct{int64(10), float64(5), tick.TokenEqual}:        false,
		keyStruct{int64(10), float64(5), tick.TokenNotEqual}:     true,
		keyStruct{int64(10), float64(5), tick.TokenGreater}:      true,
		keyStruct{int64(10), float64(5), tick.TokenGreaterEqual}: true,
		keyStruct{int64(10), float64(5), tick.TokenLess}:         false,
		keyStruct{int64(10), float64(5), tick.TokenLessEqual}:    false,

		// Left is int64(5), Right is float64(5)
		keyStruct{int64(5), float64(5), tick.TokenEqual}:        true,
		keyStruct{int64(5), float64(5), tick.TokenNotEqual}:     false,
		keyStruct{int64(5), float64(5), tick.TokenGreater}:      false,
		keyStruct{int64(5), float64(5), tick.TokenGreaterEqual}: true,
		keyStruct{int64(5), float64(5), tick.TokenLess}:         false,
		keyStruct{int64(5), float64(5), tick.TokenLessEqual}:    true,

		// Left is int64(5), Right is float64(10)
		keyStruct{int64(5), float64(10), tick.TokenEqual}:        false,
		keyStruct{int64(5), float64(10), tick.TokenNotEqual}:     true,
		keyStruct{int64(5), float64(10), tick.TokenGreater}:      false,
		keyStruct{int64(5), float64(10), tick.TokenGreaterEqual}: false,
		keyStruct{int64(5), float64(10), tick.TokenLess}:         true,
		keyStruct{int64(5), float64(10), tick.TokenLessEqual}:    true,

		// Left is int64(5), Right is int64(5)
		keyStruct{int64(5), int64(5), tick.TokenEqual}:        true,
		keyStruct{int64(5), int64(5), tick.TokenNotEqual}:     false,
		keyStruct{int64(5), int64(5), tick.TokenGreater}:      false,
		keyStruct{int64(5), int64(5), tick.TokenGreaterEqual}: true,
		keyStruct{int64(5), int64(5), tick.TokenLess}:         false,
		keyStruct{int64(5), int64(5), tick.TokenLessEqual}:    true,
	}, map[keyStruct]error{
		// Invalid operator
		keyStruct{float64(5), float64(5), tick.TokenOr}:   errors.New("invalid float64 comparison operator OR"),
		keyStruct{float64(5), float64(10), tick.TokenOr}:  errors.New("invalid float64 comparison operator OR"),
		keyStruct{float64(5), int64(5), tick.TokenOr}:     errors.New("invalid float64 comparison operator OR"),
		keyStruct{float64(10), float64(5), tick.TokenOr}:  errors.New("invalid float64 comparison operator OR"),
		keyStruct{float64(10), float64(10), tick.TokenOr}: errors.New("invalid float64 comparison operator OR"),
		keyStruct{float64(10), int64(5), tick.TokenOr}:    errors.New("invalid float64 comparison operator OR"),
		keyStruct{int64(5), float64(5), tick.TokenOr}:     errors.New("invalid int64 comparison operator OR"),
		keyStruct{int64(5), float64(10), tick.TokenOr}:    errors.New("invalid int64 comparison operator OR"),
		keyStruct{int64(5), int64(5), tick.TokenOr}:       errors.New("invalid int64 comparison operator OR"),

		// (Redundant case)
		keyStruct{float64(5), "NON_INT_VALUE", tick.TokenOr}:  errors.New("invalid float64 comparison operator OR"),
		keyStruct{float64(10), "NON_INT_VALUE", tick.TokenOr}: errors.New("invalid float64 comparison operator OR"),
		keyStruct{int64(5), "NON_INT_VALUE", tick.TokenOr}:    errors.New("invalid int64 comparison operator OR"),

		// Left is float64(5), Right is "NON_INT_VALUE"
		keyStruct{float64(5), "NON_INT_VALUE", tick.TokenEqual}:        errors.New("mismatched type to binary operator. got float64 == string. see bool(), int(), float()"),
		keyStruct{float64(5), "NON_INT_VALUE", tick.TokenNotEqual}:     errors.New("mismatched type to binary operator. got float64 != string. see bool(), int(), float()"),
		keyStruct{float64(5), "NON_INT_VALUE", tick.TokenGreater}:      errors.New("mismatched type to binary operator. got float64 > string. see bool(), int(), float()"),
		keyStruct{float64(5), "NON_INT_VALUE", tick.TokenGreaterEqual}: errors.New("mismatched type to binary operator. got float64 >= string. see bool(), int(), float()"),
		keyStruct{float64(5), "NON_INT_VALUE", tick.TokenLess}:         errors.New("mismatched type to binary operator. got float64 < string. see bool(), int(), float()"),
		keyStruct{float64(5), "NON_INT_VALUE", tick.TokenLessEqual}:    errors.New("mismatched type to binary operator. got float64 <= string. see bool(), int(), float()"),

		// (Redundant case) Left is float64(10), Right is "NON_INT_VALUE"
		keyStruct{float64(10), "NON_INT_VALUE", tick.TokenEqual}:        errors.New("mismatched type to binary operator. got float64 == string. see bool(), int(), float()"),
		keyStruct{float64(10), "NON_INT_VALUE", tick.TokenNotEqual}:     errors.New("mismatched type to binary operator. got float64 != string. see bool(), int(), float()"),
		keyStruct{float64(10), "NON_INT_VALUE", tick.TokenGreater}:      errors.New("mismatched type to binary operator. got float64 > string. see bool(), int(), float()"),
		keyStruct{float64(10), "NON_INT_VALUE", tick.TokenGreaterEqual}: errors.New("mismatched type to binary operator. got float64 >= string. see bool(), int(), float()"),
		keyStruct{float64(10), "NON_INT_VALUE", tick.TokenLess}:         errors.New("mismatched type to binary operator. got float64 < string. see bool(), int(), float()"),
		keyStruct{float64(10), "NON_INT_VALUE", tick.TokenLessEqual}:    errors.New("mismatched type to binary operator. got float64 <= string. see bool(), int(), float()"),

		// Left is int64(5), Right is "NON_INT_VALUE"
		keyStruct{int64(5), "NON_INT_VALUE", tick.TokenEqual}:        errors.New("mismatched type to binary operator. got int64 == string. see bool(), int(), float()"),
		keyStruct{int64(5), "NON_INT_VALUE", tick.TokenNotEqual}:     errors.New("mismatched type to binary operator. got int64 != string. see bool(), int(), float()"),
		keyStruct{int64(5), "NON_INT_VALUE", tick.TokenGreater}:      errors.New("mismatched type to binary operator. got int64 > string. see bool(), int(), float()"),
		keyStruct{int64(5), "NON_INT_VALUE", tick.TokenGreaterEqual}: errors.New("mismatched type to binary operator. got int64 >= string. see bool(), int(), float()"),
		keyStruct{int64(5), "NON_INT_VALUE", tick.TokenLess}:         errors.New("mismatched type to binary operator. got int64 < string. see bool(), int(), float()"),
		keyStruct{int64(5), "NON_INT_VALUE", tick.TokenLessEqual}:    errors.New("mismatched type to binary operator. got int64 <= string. see bool(), int(), float()"),
	})
}

func TestExpression_EvalBool_StringNode(t *testing.T) {
	leftValues := []interface{}{"a", "b"}
	rightValues := []interface{}{"a", "b", int64(123)}
	operators := []tick.TokenType{tick.TokenEqual, tick.TokenNotEqual, tick.TokenGreater, tick.TokenGreaterEqual, tick.TokenLessEqual, tick.TokenLess, tick.TokenOr}

	createStringNode := func(v interface{}) tick.Node {
		switch value := v.(type) {
		case string:
			return &tick.StringNode{
				Literal: value,
			}
		case int64:
			return &tick.NumberNode{
				IsInt: true,
				Int64: value,
			}
		default:
			t.Fatalf("value supplied to createStringNode is not string/int64: %t", v)
			return nil
		}
	}

	runCompiledEvalBoolTests(t, createStringNode, leftValues, rightValues, operators, map[keyStruct]interface{}{
		// Left is "a", Right is "a"
		keyStruct{"a", "a", tick.TokenEqual}:        true,
		keyStruct{"a", "a", tick.TokenNotEqual}:     false,
		keyStruct{"a", "a", tick.TokenGreater}:      false,
		keyStruct{"a", "a", tick.TokenGreaterEqual}: true,
		keyStruct{"a", "a", tick.TokenLess}:         false,
		keyStruct{"a", "a", tick.TokenLessEqual}:    true,

		// Left is "a", Right is "b"
		keyStruct{"a", "b", tick.TokenEqual}:        false,
		keyStruct{"a", "b", tick.TokenNotEqual}:     true,
		keyStruct{"a", "b", tick.TokenGreater}:      false,
		keyStruct{"a", "b", tick.TokenGreaterEqual}: false,
		keyStruct{"a", "b", tick.TokenLess}:         true,
		keyStruct{"a", "b", tick.TokenLessEqual}:    true,

		// Left is "b", Right is "a"
		keyStruct{"b", "a", tick.TokenEqual}:        false,
		keyStruct{"b", "a", tick.TokenNotEqual}:     true,
		keyStruct{"b", "a", tick.TokenGreater}:      true,
		keyStruct{"b", "a", tick.TokenGreaterEqual}: true,
		keyStruct{"b", "a", tick.TokenLess}:         false,
		keyStruct{"b", "a", tick.TokenLessEqual}:    false,

		// Left is "b", Right is "b"
		keyStruct{"b", "b", tick.TokenEqual}:        true,
		keyStruct{"b", "b", tick.TokenNotEqual}:     false,
		keyStruct{"b", "b", tick.TokenGreater}:      false,
		keyStruct{"b", "b", tick.TokenGreaterEqual}: true,
		keyStruct{"b", "b", tick.TokenLess}:         false,
		keyStruct{"b", "b", tick.TokenLessEqual}:    true,
	}, map[keyStruct]error{
		// Invalid operator
		keyStruct{"a", "a", tick.TokenOr}: errors.New("invalid string comparison operator OR"),
		keyStruct{"a", "b", tick.TokenOr}: errors.New("invalid string comparison operator OR"),
		keyStruct{"b", "a", tick.TokenOr}: errors.New("invalid string comparison operator OR"),
		keyStruct{"b", "b", tick.TokenOr}: errors.New("invalid string comparison operator OR"),

		keyStruct{"a", int64(123), tick.TokenOr}: errors.New("invalid string comparison operator OR"),
		keyStruct{"b", int64(123), tick.TokenOr}: errors.New("invalid string comparison operator OR"),

		// Left is "a", Right is int64(123)
		keyStruct{"a", int64(123), tick.TokenEqual}:        errors.New("mismatched type to binary operator. got string == int64. see bool(), int(), float()"),
		keyStruct{"a", int64(123), tick.TokenNotEqual}:     errors.New("mismatched type to binary operator. got string != int64. see bool(), int(), float()"),
		keyStruct{"a", int64(123), tick.TokenGreater}:      errors.New("mismatched type to binary operator. got string > int64. see bool(), int(), float()"),
		keyStruct{"a", int64(123), tick.TokenGreaterEqual}: errors.New("mismatched type to binary operator. got string >= int64. see bool(), int(), float()"),
		keyStruct{"a", int64(123), tick.TokenLess}:         errors.New("mismatched type to binary operator. got string < int64. see bool(), int(), float()"),
		keyStruct{"a", int64(123), tick.TokenLessEqual}:    errors.New("mismatched type to binary operator. got string <= int64. see bool(), int(), float()"),

		// Left is "b", Right is int64(123)
		keyStruct{"b", int64(123), tick.TokenEqual}:        errors.New("mismatched type to binary operator. got string == int64. see bool(), int(), float()"),
		keyStruct{"b", int64(123), tick.TokenNotEqual}:     errors.New("mismatched type to binary operator. got string != int64. see bool(), int(), float()"),
		keyStruct{"b", int64(123), tick.TokenGreater}:      errors.New("mismatched type to binary operator. got string > int64. see bool(), int(), float()"),
		keyStruct{"b", int64(123), tick.TokenGreaterEqual}: errors.New("mismatched type to binary operator. got string >= int64. see bool(), int(), float()"),
		keyStruct{"b", int64(123), tick.TokenLess}:         errors.New("mismatched type to binary operator. got string < int64. see bool(), int(), float()"),
		keyStruct{"b", int64(123), tick.TokenLessEqual}:    errors.New("mismatched type to binary operator. got string <= int64. see bool(), int(), float()"),
	})
}

func TestExpression_EvalBool_RegexNode(t *testing.T) {
	leftValues := []interface{}{"abc", "cba"}

	// Right values are regex, but we are supplying strings because the keyStruct and maps don't play nice together
	// so we mark regex with prefix of "R!" and createStringOrRegexNode will convert it to regex
	rightValues := []interface{}{"R!^(.*)c$"}
	operators := []tick.TokenType{tick.TokenRegexEqual, tick.TokenRegexNotEqual, tick.TokenEqual}

	createStringOrRegexNode := func(v interface{}) tick.Node {
		stringValue := v.(string)
		if strings.Index(stringValue, "R!") == 0 {
			return &tick.RegexNode{
				Regex: regexp.MustCompile(strings.TrimPrefix(stringValue, "R!")),
			}
		}

		return &tick.StringNode{
			Literal: stringValue,
		}

	}

	runCompiledEvalBoolTests(t, createStringOrRegexNode, leftValues, rightValues, operators, map[keyStruct]interface{}{
		// Left is "abc", Right is regex "(.*)c"
		keyStruct{"abc", "R!^(.*)c$", tick.TokenRegexEqual}:    true,
		keyStruct{"abc", "R!^(.*)c$", tick.TokenRegexNotEqual}: false,

		// Left is "cba", Right is regex "(.*)c"
		keyStruct{"cba", "R!^(.*)c$", tick.TokenRegexEqual}:    false,
		keyStruct{"cba", "R!^(.*)c$", tick.TokenRegexNotEqual}: true,
	},
		map[keyStruct]error{
			// Errors for invalid operators
			keyStruct{"abc", "R!^(.*)c$", tick.TokenEqual}: errors.New("invalid regex comparison operator =="),
			keyStruct{"cba", "R!^(.*)c$", tick.TokenEqual}: errors.New("invalid regex comparison operator =="),
		})
}

func TestExpression_EvalBool_NotSupportedValueLeft(t *testing.T) {
	scope := tick.NewScope()
	scope.Set("value", []int{1, 2, 3})
	_, err := evalCompiledBoolWithScope(t, scope, &tick.BinaryNode{
		Operator: tick.TokenEqual,
		Left: &tick.ReferenceNode{
			Reference: "value",
		},
		Right: &tick.StringNode{
			Literal: "yo",
		},
	})

	expectedError := "left value is invalid value type"

	if err != nil && (err.Error() != expectedError) {
		t.Errorf("Unexpected error result: \ngot: %v\nexpected: %v", err.Error(), expectedError)
	}

	if err == nil {
		t.Error("Unexpected error result: but didn't got any error")
	}

	// Swap sides
	_, err = evalCompiledBoolWithScope(t, scope, &tick.BinaryNode{
		Operator: tick.TokenEqual,
		Left: &tick.StringNode{
			Literal: "yo",
		},
		Right: &tick.ReferenceNode{
			Reference: "value",
		},
	})

	expectedError = "right value is invalid value type"

	if err != nil && (err.Error() != expectedError) {
		t.Errorf("Unexpected error result: \ngot: %v\nexpected: %v", err.Error(), expectedError)
	}

	if err == nil {
		t.Error("Unexpected error result: but didn't got any error")
	}
}

func TestExpression_EvalBool_UnknownOperator(t *testing.T) {
	_, err := evalCompiledBoolWithScope(t, tick.NewScope(), &tick.BinaryNode{
		Operator: tick.TokenType(666),
		Left: &tick.StringNode{
			Literal: "value",
		},
		Right: &tick.StringNode{
			Literal: "yo",
		},
	})

	expectedError := "return: unknown operator 666"

	if err != nil && (err.Error() != expectedError) {
		t.Errorf("Unexpected error result: \ngot: %v\nexpected: %v", err.Error(), expectedError)
	}

	if err == nil {
		t.Error("Unexpected error result: but didn't got any error")
	}
}

func TestExpression_EvalBool_ReferenceNodeDosentExist(t *testing.T) {
	emptyScope := tick.NewScope()
	expectedError := `name "value" is undefined. Names in scope: `

	// Check left side
	_, err := evalCompiledBoolWithScope(t, emptyScope, &tick.BinaryNode{
		Operator: tick.TokenEqual,
		Left: &tick.ReferenceNode{
			Reference: "value",
		},
		Right: &tick.StringNode{
			Literal: "yo",
		},
	})

	if err != nil && (err.Error() != expectedError) {
		t.Errorf("Unexpected error result: \ngot: %v\nexpected: %v", err.Error(), expectedError)
	}

	if err == nil {
		t.Error("Unexpected error result: but didn't got any error")
	}

	// Check right side
	_, err = evalCompiledBoolWithScope(t, emptyScope, &tick.BinaryNode{
		Operator: tick.TokenEqual,
		Left: &tick.StringNode{
			Literal: "yo",
		},
		Right: &tick.ReferenceNode{
			Reference: "value",
		},
	})

	if err != nil && (err.Error() != expectedError) {
		t.Errorf("Unexpected error result: \ngot: %v\nexpected: %v", err.Error(), expectedError)
	}

	if err == nil {
		t.Error("Unexpected error result: but didn't got any error")
	}
}

func TestExpression_EvalBool_ReferenceNodeNil(t *testing.T) {
	scope := tick.NewScope()
	scope.Set("value", nil)

	expectedError := `referenced value "value" is nil.`

	// Check left side
	_, err := evalCompiledBoolWithScope(t, scope, &tick.BinaryNode{
		Operator: tick.TokenEqual,
		Left: &tick.ReferenceNode{
			Reference: "value",
		},
		Right: &tick.StringNode{
			Literal: "yo",
		},
	})

	if err != nil && (err.Error() != expectedError) {
		t.Errorf("Unexpected error result: \ngot: %v\nexpected: %v", err.Error(), expectedError)
	}

	if err == nil {
		t.Error("Unexpected error result: but didn't got any error")
	}

	// Check right side
	_, err = evalCompiledBoolWithScope(t, scope, &tick.BinaryNode{
		Operator: tick.TokenEqual,
		Left: &tick.StringNode{
			Literal: "yo",
		},
		Right: &tick.ReferenceNode{
			Reference: "value",
		},
	})

	if err != nil && (err.Error() != expectedError) {
		t.Errorf("Unexpected error result: \ngot: %v\nexpected: %v", err.Error(), expectedError)
	}

	if err == nil {
		t.Error("Unexpected error result: but didn't got any error")
	}
}

func TestExpression_EvalBool_ReturnsReferenceNode(t *testing.T) {
	scope := tick.NewScope()

	// First Case - true as boolValue
	boolValue := true

	scope.Set("boolValue", boolValue)
	result, err := evalCompiledBoolWithScope(t, scope, &tick.ReferenceNode{
		Reference: "boolValue",
	})

	if err != nil {
		t.Errorf("Unexpected error result: %v", err.Error())
	}

	if result != boolValue {
		t.Errorf("Unexpected result: \ngot: %v\nexp: %v", result, boolValue)
	}

	// Second Case - false as boolValue
	boolValue = false

	scope.Set("boolValue", boolValue)
	result, err = evalCompiledBoolWithScope(t, scope, &tick.ReferenceNode{
		Reference: "boolValue",
	})

	if err != nil {
		t.Errorf("Unexpected error result: %v", err.Error())
	}

	if result != boolValue {
		t.Errorf("Unexpected result: \ngot: %v\nexp: %v", result, boolValue)
	}
}

func TestExpression_EvalNum_ReferenceNodeDosentExist(t *testing.T) {
	emptyScope := tick.NewScope()
	expectedError := `name "value" is undefined. Names in scope: `

	// Check left side
	se := mustCompileExpression(t, &tick.ReferenceNode{
		Reference: "value",
	})

	result, err := se.EvalNum(emptyScope)

	if err != nil && (err.Error() != expectedError) {
		t.Errorf("Unexpected error result: \ngot: %v\nexpected: %v", err.Error(), expectedError)
	}

	if err == nil {
		t.Errorf("Expected error result: but didn't got any error, got result: %v", result)
	}
}

func TestExpression_EvalBool_UnexpectedTypeResult(t *testing.T) {
	expectedError := `expression returned unexpected type invalid type`

	scope := tick.NewScope()
	scope.Set("value", []int{1, 2, 3})

	// Check left side
	_, err := evalCompiledBoolWithScope(t, scope, &tick.ReferenceNode{
		Reference: "value",
	})

	if err != nil && (err.Error() != expectedError) {
		t.Errorf("Unexpected error result: \ngot: %v\nexpected: %v", err.Error(), expectedError)
	}

	if err == nil {
		t.Error("Unexpected error result: but didn't got any error")
	}
}

func TestExpression_EvalBool_ReferenceNodeDosentExistInBinaryNode(t *testing.T) {
	emptyScope := tick.NewScope()
	expectedError := `name "value" is undefined. Names in scope: `

	// Check left side
	_, err := evalCompiledBoolWithScope(t, emptyScope, &tick.BinaryNode{
		Operator: tick.TokenGreater,
		Left: &tick.ReferenceNode{
			Reference: "value",
		},
		Right: &tick.NumberNode{
			IsInt: true,
			Int64: int64(0),
		},
	})

	if err != nil && (err.Error() != expectedError) {
		t.Errorf("Unexpected error result: \ngot: %v\nexpected: %v", err.Error(), expectedError)
	}

	if err == nil {
		t.Error("Unexpected error result: but didn't got any error")
	}
}

func TestExpression_EvalNum_BinaryNodeWithUnary(t *testing.T) {

	// -"value" < 0 , yes, of course, this is always true..
	se := mustCompileExpression(t, &tick.BinaryNode{
		Operator: tick.TokenLess,
		Left: &tick.UnaryNode{
			Operator: tick.TokenMinus,
			Node: &tick.ReferenceNode{
				Reference: "value",
			},
		},
		Right: &tick.NumberNode{
			IsInt: true,
			Int64: int64(0),
		},
	})

	scope := tick.NewScope()
	scope.Set("value", int64(4))
	result, err := se.EvalBool(scope)
	if err != nil {
		t.Errorf("Ref node: Failed to evaluate:\n%v", err)
	}

	if !result {
		t.Errorf("int64 ref test case: unexpected result: got: %t, expected: true", result)
	}

}

func TestExpression_EvalBool_BinaryNodeWithBoolUnaryNode(t *testing.T) {

	emptyScope := tick.NewScope()

	se := mustCompileExpression(t, &tick.BinaryNode{
		Operator: tick.TokenEqual,
		Left: &tick.UnaryNode{
			Operator: tick.TokenNot,
			Node: &tick.BoolNode{
				Bool: false,
			},
		},
		Right: &tick.BoolNode{
			Bool: true,
		},
	})

	result, err := se.EvalBool(emptyScope)
	if err != nil {
		t.Errorf("first case: %v", err)
	}

	if !result {
		t.Errorf("first case: unexpected result: got: %t, expected: true", result)
	}

	// now with ref
	se = mustCompileExpression(t, &tick.BinaryNode{
		Operator: tick.TokenEqual,
		Left: &tick.UnaryNode{
			Operator: tick.TokenNot,
			Node: &tick.ReferenceNode{
				Reference: "value",
			},
		},
		Right: &tick.BoolNode{
			Bool: true,
		},
	})

	scope := tick.NewScope()
	scope.Set("value", bool(false))

	result, err = se.EvalBool(scope)
	if err != nil {
		t.Errorf("ref case: %v", err)
	}

	if !result {
		t.Errorf("ref case: unexpected result: got: %t, expected: true", result)
	}

}

func TestExpression_EvalBool_BinaryNodeWithNumericUnaryNode(t *testing.T) {

	scope := tick.NewScope()

	se := mustCompileExpression(t, &tick.BinaryNode{
		Operator: tick.TokenLess,
		Left: &tick.UnaryNode{
			Operator: tick.TokenMinus,
			Node: &tick.NumberNode{
				IsInt: true,
				Int64: 4,
			},
		},
		Right: &tick.NumberNode{
			IsInt: true,
			Int64: 0,
		},
	})

	result, err := se.EvalBool(scope)
	if err != nil {
		t.Error(err)
	}

	if !result {
		t.Errorf("unexpected result: got: %t, expected: true", result)
	}

}

func TestExpression_EvalBool_TwoLevelsDeepBinary(t *testing.T) {

	scope := tick.NewScope()

	// passing
	scope.Set("a", int64(11))
	scope.Set("b", int64(5))

	// a > 10 and b < 10
	se := mustCompileExpression(t, &tick.BinaryNode{
		Operator: tick.TokenAnd,

		Left: &tick.BinaryNode{
			Operator: tick.TokenGreater,
			Left: &tick.ReferenceNode{
				Reference: "a",
			},
			Right: &tick.NumberNode{
				IsInt: true,
				Int64: 10,
			},
		},

		Right: &tick.BinaryNode{
			Operator: tick.TokenLess,
			Left: &tick.ReferenceNode{
				Reference: "b",
			},
			Right: &tick.NumberNode{
				IsInt: true,
				Int64: 10,
			},
		},
	})

	result, err := se.EvalBool(scope)
	if err != nil {
		t.Error(err)
	}

	if !result {
		t.Errorf("unexpected result: got: %t, expected: true", result)
	}

	// fail
	scope.Set("a", int64(6))

	result, err = se.EvalBool(scope)
	if err != nil {
		t.Error(err)
	}

	if result {
		t.Errorf("unexpected result: got: %t, expected: false", result)
	}
}

func TestExpression_EvalBool_TwoLevelsDeepBinaryWithEvalNum_Int64(t *testing.T) {

	scope := tick.NewScope()

	// passing
	scope.Set("a", int64(11))
	scope.Set("b", int64(5))

	// a > 10 and b < 10
	se := mustCompileExpression(t, &tick.BinaryNode{
		Operator: tick.TokenAnd,

		Left: &tick.BinaryNode{
			Operator: tick.TokenGreater,
			Left: &tick.ReferenceNode{
				Reference: "a",
			},
			// right = 5 * 2 = 10
			Right: &tick.BinaryNode{
				Operator: tick.TokenMult,
				Left: &tick.NumberNode{
					IsInt: true,
					Int64: 5,
				},
				Right: &tick.NumberNode{
					IsInt: true,
					Int64: 2,
				},
			},
		},

		Right: &tick.BinaryNode{
			Operator: tick.TokenLess,
			Left: &tick.ReferenceNode{
				Reference: "b",
			},
			Right: &tick.NumberNode{
				IsInt: true,
				Int64: 10,
			},
		},
	})

	result, err := se.EvalBool(scope)
	if err != nil {
		t.Error(err)
	}

	if !result {
		t.Errorf("unexpected result: got: %t, expected: true", result)
	}

	// fail
	scope.Set("a", int64(6))

	result, err = se.EvalBool(scope)
	if err != nil {
		t.Error(err)
	}

	if result {
		t.Errorf("unexpected result: got: %t, expected: false", result)
	}
}

func TestExpression_EvalBool_TwoLevelsDeepBinaryWithEvalNum_Float64(t *testing.T) {

	scope := tick.NewScope()

	// passing
	scope.Set("a", float64(11))
	scope.Set("b", float64(5))

	// a > 10 and b < 10
	se := mustCompileExpression(t, &tick.BinaryNode{
		Operator: tick.TokenAnd,

		Left: &tick.BinaryNode{
			Operator: tick.TokenGreater,
			Left: &tick.ReferenceNode{
				Reference: "a",
			},
			// right = 5 * 2 = 10
			Right: &tick.BinaryNode{
				Operator: tick.TokenMult,
				Left: &tick.NumberNode{
					IsFloat: true,
					Float64: 5,
				},
				Right: &tick.NumberNode{
					IsFloat: true,
					Float64: 2,
				},
			},
		},

		Right: &tick.BinaryNode{
			Operator: tick.TokenLess,
			Left: &tick.ReferenceNode{
				Reference: "b",
			},
			Right: &tick.NumberNode{
				IsFloat: true,
				Float64: 10,
			},
		},
	})

	result, err := se.EvalBool(scope)
	if err != nil {
		t.Error(err)
	}

	if !result {
		t.Errorf("unexpected result: got: %t, expected: true", result)
	}

	// fail
	scope.Set("a", float64(6))

	result, err = se.EvalBool(scope)
	if err != nil {
		t.Error(err)
	}

	if result {
		t.Errorf("unexpected result: got: %t, expected: false", result)
	}
}

func TestExpression_EvalNum_NumberNode(t *testing.T) {
	leftValues := []interface{}{float64(5), float64(10), int64(5)}
	rightValues := []interface{}{float64(5), float64(10), int64(5), "NON_INT_VALUE"}

	operators := []tick.TokenType{
		tick.TokenPlus,
		tick.TokenMinus,
		tick.TokenMult,
		tick.TokenDiv,
		tick.TokenMod,
	}

	createNumberNode := func(v interface{}) tick.Node {
		switch value := v.(type) {
		case float64:
			return &tick.NumberNode{
				IsFloat: true,
				Float64: value,
			}
		case int64:
			return &tick.NumberNode{
				IsInt: true,
				Int64: value,
			}
		// For the error case
		case string:
			return &tick.StringNode{
				Literal: value,
			}
		default:
			t.Fatalf("value supplied to createNumberNode is not string/int64/float64: %t", v)
			return nil
		}
	}

	runCompiledNumericTests(t, createNumberNode, leftValues, rightValues, operators, map[keyStruct]interface{}{
		// Left is float64(5), Right is float64(5)
		keyStruct{float64(5), float64(5), tick.TokenPlus}:  float64(10),
		keyStruct{float64(5), float64(5), tick.TokenMinus}: float64(0),
		keyStruct{float64(5), float64(5), tick.TokenMult}:  float64(25),
		keyStruct{float64(5), float64(5), tick.TokenDiv}:   float64(1),

		// Left is int64(5), Right is int64(5)
		keyStruct{int64(5), int64(5), tick.TokenPlus}:  int64(10),
		keyStruct{int64(5), int64(5), tick.TokenMinus}: int64(0),
		keyStruct{int64(5), int64(5), tick.TokenMult}:  int64(25),
		keyStruct{int64(5), int64(5), tick.TokenDiv}:   int64(1),
		keyStruct{int64(5), int64(5), tick.TokenMod}:   int64(0),

		// Left is float64(5), Right is float64(10)
		keyStruct{float64(5), float64(10), tick.TokenPlus}:  float64(15),
		keyStruct{float64(5), float64(10), tick.TokenMinus}: float64(-5),
		keyStruct{float64(5), float64(10), tick.TokenMult}:  float64(50),
		keyStruct{float64(5), float64(10), tick.TokenDiv}:   float64(0.5),

		// Left is float64(10), Right is float64(5)
		keyStruct{float64(10), float64(5), tick.TokenPlus}:  float64(15),
		keyStruct{float64(10), float64(5), tick.TokenMinus}: float64(5),
		keyStruct{float64(10), float64(5), tick.TokenMult}:  float64(50),
		keyStruct{float64(10), float64(5), tick.TokenDiv}:   float64(2),

		// Left is float64(10), Right is float64(10)
		keyStruct{float64(10), float64(10), tick.TokenPlus}:  float64(20),
		keyStruct{float64(10), float64(10), tick.TokenMinus}: float64(0),
		keyStruct{float64(10), float64(10), tick.TokenMult}:  float64(100),
		keyStruct{float64(10), float64(10), tick.TokenDiv}:   float64(1),
	}, map[keyStruct]error{
		// Modulo token where left is float
		keyStruct{float64(5), float64(5), tick.TokenMod}:       errors.New("invalid float64 math operator %"),
		keyStruct{float64(5), float64(10), tick.TokenMod}:      errors.New("invalid float64 math operator %"),
		keyStruct{float64(10), float64(5), tick.TokenMod}:      errors.New("invalid float64 math operator %"),
		keyStruct{float64(10), float64(10), tick.TokenMod}:     errors.New("invalid float64 math operator %"),
		keyStruct{float64(5), int64(5), tick.TokenMod}:         errors.New("invalid float64 math operator %"),
		keyStruct{float64(10), int64(5), tick.TokenMod}:        errors.New("invalid float64 math operator %"),
		keyStruct{float64(10), "NON_INT_VALUE", tick.TokenMod}: errors.New("invalid float64 math operator %"),
		keyStruct{float64(5), "NON_INT_VALUE", tick.TokenMod}:  errors.New("invalid float64 math operator %"),

		// Left is int, right is float
		keyStruct{int64(5), float64(5), tick.TokenPlus}:   errors.New("mismatched type to binary operator. got int64 + float64. see bool(), int(), float()"),
		keyStruct{int64(5), float64(5), tick.TokenMinus}:  errors.New("mismatched type to binary operator. got int64 - float64. see bool(), int(), float()"),
		keyStruct{int64(5), float64(5), tick.TokenMult}:   errors.New("mismatched type to binary operator. got int64 * float64. see bool(), int(), float()"),
		keyStruct{int64(5), float64(5), tick.TokenDiv}:    errors.New("mismatched type to binary operator. got int64 / float64. see bool(), int(), float()"),
		keyStruct{int64(5), float64(5), tick.TokenMod}:    errors.New("mismatched type to binary operator. got int64 % float64. see bool(), int(), float()"),
		keyStruct{int64(5), float64(10), tick.TokenPlus}:  errors.New("mismatched type to binary operator. got int64 + float64. see bool(), int(), float()"),
		keyStruct{int64(5), float64(10), tick.TokenMinus}: errors.New("mismatched type to binary operator. got int64 - float64. see bool(), int(), float()"),
		keyStruct{int64(5), float64(10), tick.TokenMult}:  errors.New("mismatched type to binary operator. got int64 * float64. see bool(), int(), float()"),
		keyStruct{int64(5), float64(10), tick.TokenDiv}:   errors.New("mismatched type to binary operator. got int64 / float64. see bool(), int(), float()"),
		keyStruct{int64(5), float64(10), tick.TokenMod}:   errors.New("mismatched type to binary operator. got int64 % float64. see bool(), int(), float()"),

		// Left is float, right is int
		keyStruct{float64(5), int64(5), tick.TokenPlus}:  errors.New("mismatched type to binary operator. got float64 + int64. see bool(), int(), float()"),
		keyStruct{float64(5), int64(5), tick.TokenMinus}: errors.New("mismatched type to binary operator. got float64 - int64. see bool(), int(), float()"),
		keyStruct{float64(5), int64(5), tick.TokenMult}:  errors.New("mismatched type to binary operator. got float64 * int64. see bool(), int(), float()"),
		keyStruct{float64(5), int64(5), tick.TokenDiv}:   errors.New("mismatched type to binary operator. got float64 / int64. see bool(), int(), float()"),

		keyStruct{float64(10), int64(5), tick.TokenPlus}:  errors.New("mismatched type to binary operator. got float64 + int64. see bool(), int(), float()"),
		keyStruct{float64(10), int64(5), tick.TokenMinus}: errors.New("mismatched type to binary operator. got float64 - int64. see bool(), int(), float()"),
		keyStruct{float64(10), int64(5), tick.TokenMult}:  errors.New("mismatched type to binary operator. got float64 * int64. see bool(), int(), float()"),
		keyStruct{float64(10), int64(5), tick.TokenDiv}:   errors.New("mismatched type to binary operator. got float64 / int64. see bool(), int(), float()"),

		// Left is int64, Right is "NON_INT_VALUE"
		keyStruct{int64(5), "NON_INT_VALUE", tick.TokenPlus}:  errors.New("mismatched type to binary operator. got int64 + string. see bool(), int(), float()"),
		keyStruct{int64(5), "NON_INT_VALUE", tick.TokenMinus}: errors.New("mismatched type to binary operator. got int64 - string. see bool(), int(), float()"),
		keyStruct{int64(5), "NON_INT_VALUE", tick.TokenMult}:  errors.New("mismatched type to binary operator. got int64 * string. see bool(), int(), float()"),
		keyStruct{int64(5), "NON_INT_VALUE", tick.TokenDiv}:   errors.New("mismatched type to binary operator. got int64 / string. see bool(), int(), float()"),
		keyStruct{int64(5), "NON_INT_VALUE", tick.TokenMod}:   errors.New("mismatched type to binary operator. got int64 % string. see bool(), int(), float()"),

		// Left is float64, Right is "NON_INT_VALUE"
		keyStruct{float64(5), "NON_INT_VALUE", tick.TokenPlus}:   errors.New("mismatched type to binary operator. got float64 + string. see bool(), int(), float()"),
		keyStruct{float64(5), "NON_INT_VALUE", tick.TokenMinus}:  errors.New("mismatched type to binary operator. got float64 - string. see bool(), int(), float()"),
		keyStruct{float64(5), "NON_INT_VALUE", tick.TokenMult}:   errors.New("mismatched type to binary operator. got float64 * string. see bool(), int(), float()"),
		keyStruct{float64(5), "NON_INT_VALUE", tick.TokenDiv}:    errors.New("mismatched type to binary operator. got float64 / string. see bool(), int(), float()"),
		keyStruct{float64(10), "NON_INT_VALUE", tick.TokenPlus}:  errors.New("mismatched type to binary operator. got float64 + string. see bool(), int(), float()"),
		keyStruct{float64(10), "NON_INT_VALUE", tick.TokenMinus}: errors.New("mismatched type to binary operator. got float64 - string. see bool(), int(), float()"),
		keyStruct{float64(10), "NON_INT_VALUE", tick.TokenMult}:  errors.New("mismatched type to binary operator. got float64 * string. see bool(), int(), float()"),
		keyStruct{float64(10), "NON_INT_VALUE", tick.TokenDiv}:   errors.New("mismatched type to binary operator. got float64 / string. see bool(), int(), float()"),
	})
}

func runCompiledNumericTests(
	t *testing.T,
	createNodeFn func(v interface{}) tick.Node,
	leftValues []interface{},
	rightValues []interface{},
	operators []tick.TokenType,
	expected map[keyStruct]interface{},
	errorExpectations map[keyStruct]error) {

	runCompiledEvalTests(t, func(t *testing.T, scope *tick.Scope, n tick.Node) (interface{}, error) {
		se := mustCompileExpression(t, n)
		return se.EvalNum(scope)
	}, createNodeFn, leftValues, rightValues, operators, expected, errorExpectations)
}

func runCompiledEvalBoolTests(
	t *testing.T,
	createNodeFn func(v interface{}) tick.Node,
	leftValues []interface{},
	rightValues []interface{},
	operators []tick.TokenType,
	expected map[keyStruct]interface{},
	errorExpectations map[keyStruct]error) {

	runCompiledEvalTests(t, evalCompiledBoolWithScope, createNodeFn, leftValues, rightValues, operators, expected, errorExpectations)
}

func evalCompiledBoolWithScope(t *testing.T, scope *tick.Scope, n tick.Node) (interface{}, error) {
	se := mustCompileExpression(t, n)
	return se.EvalBool(scope)
}

func runCompiledEvalTests(
	t *testing.T,
	evalNodeFn func(t *testing.T, scope *tick.Scope, n tick.Node) (interface{}, error),
	createNodeFn func(v interface{}) tick.Node,
	leftValues []interface{},
	rightValues []interface{},
	operators []tick.TokenType,
	expected map[keyStruct]interface{},
	errorExpectations map[keyStruct]error) {

	for _, lhs := range leftValues {
		for _, rhs := range rightValues {
			for _, op := range operators {

				key := keyStruct{lhs, rhs, op}
				exp, isExpectedResultOk := expected[key]
				errorExpected, isErrorOk := errorExpectations[key]
				if !isExpectedResultOk && !isErrorOk {
					t.Fatalf("Couldn't find an expected result/error for: lhs: %t, rhs: %t, op: %v", lhs, rhs, op)
				}

				// Test simple const values compares
				emptyScope := tick.NewScope()
				result, err := evalNodeFn(t, emptyScope, &tick.BinaryNode{
					Operator: op,
					Left:     createNodeFn(lhs),
					Right:    createNodeFn(rhs),
				})

				// This is bool matching, but not error matching..
				if isExpectedResultOk && !isErrorOk && err != nil {
					t.Errorf("Got an error while evaluating: %t %v %t - %v\n", lhs, op, rhs, err)
				} else {

					// Expect value can be error or bool
					if isErrorOk && errorExpected.Error() != err.Error() {
						t.Errorf("unexpected error result: %t %v %t\ngot: %v\nexp: %v", lhs, op, rhs, err, errorExpected)
					} else if isExpectedResultOk && exp != result {
						t.Errorf("unexpected result: %t %v %t\ngot: %v\nexp: %v", lhs, op, rhs, result, exp)
					}
				}

				// Test left is reference while the right is const
				scope := tick.NewScope()
				scope.Set("value", lhs)
				result, err = evalNodeFn(t, scope, &tick.BinaryNode{
					Operator: op,
					Left: &tick.ReferenceNode{
						Reference: "value",
					},
					Right: createNodeFn(rhs),
				})

				if isErrorOk {
					if err == nil {
						t.Errorf("reference test: expected an error but got result: %t %v %t\nresult: %t\nerr: %v", lhs, op, rhs, result, err)
					} else if errorExpected.Error() != err.Error() {
						t.Errorf("reference test: unexpected error result: %t %v %t\ngot: %v\nexp: %v", lhs, op, rhs, err, errorExpected)
					}
				} else if isExpectedResultOk && exp != result {
					t.Errorf("reference test: unexpected bool result: %t %v %t\ngot: %t\nexp: %t", lhs, op, rhs, result, exp)
				}

			}

		}
	}
}

func mustCompileExpression(t *testing.T, node tick.Node) stateful.Expression {
	se, err := stateful.NewExpression(node)
	if err != nil {
		t.Fatalf("Failed to compile expression: %v", err)
	}

	return se
}
