package stateful_test

import (
	"errors"
	"fmt"
	"regexp"
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
	se := mustCompileExpression(&tick.FunctionNode{
		Func: "sigma",
		Args: []tick.Node{&tick.ReferenceNode{Reference: "value"}},
	})

	// first
	scope := tick.NewScope()
	scope.Set("value", float64(97.1))
	result, err := se.Eval(scope)
	if err != nil {
		t.Errorf("First: Got unexpected error: %v", err)
	}

	if result != float64(0) {
		t.Errorf("First: expected count to be math.NaN() but got %v", result)
	}

	// second
	scope.Set("value", float64(92.6))
	result, err = se.Eval(scope)
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
	se := mustCompileExpression(&tick.BinaryNode{
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
}

func TestExpression_EvalBool_BoolNode(t *testing.T) {
	leftValues := []interface{}{true, false}

	// Right values are the same as left, just add a mismatch case
	rightValues := []interface{}{true, false, "NON_BOOL_VALUE"}
	operators := []tick.TokenType{tick.TokenEqual, tick.TokenNotEqual, tick.TokenAnd, tick.TokenOr, tick.TokenLess}

	createBoolNode := func(v interface{}) tick.Node {
		switch value := v.(type) {
		case bool:
			return &tick.BoolNode{
				Bool: value,
			}
		case string:
			return &tick.StringNode{
				Literal: value,
			}
		default:
			panic(fmt.Sprintf("unexpected type %T", v))
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
		keyStruct{true, true, tick.TokenLess}:   errors.New("invalid comparison operator < for type boolean"),
		keyStruct{true, false, tick.TokenLess}:  errors.New("invalid comparison operator < for type boolean"),
		keyStruct{false, true, tick.TokenLess}:  errors.New("invalid comparison operator < for type boolean"),
		keyStruct{false, false, tick.TokenLess}: errors.New("invalid comparison operator < for type boolean"),

		// (Redundant test case)
		keyStruct{true, "NON_BOOL_VALUE", tick.TokenLess}:  errors.New("invalid comparison operator < for type boolean"),
		keyStruct{false, "NON_BOOL_VALUE", tick.TokenLess}: errors.New("invalid comparison operator < for type boolean"),

		// Left: True, Right: "NON_BOOL_VALUE"
		keyStruct{true, "NON_BOOL_VALUE", tick.TokenEqual}:    errors.New("mismatched type to binary operator. got boolean == string. see bool(), int(), float(), string()"),
		keyStruct{true, "NON_BOOL_VALUE", tick.TokenNotEqual}: errors.New("mismatched type to binary operator. got boolean != string. see bool(), int(), float(), string()"),
		keyStruct{true, "NON_BOOL_VALUE", tick.TokenAnd}:      errors.New("mismatched type to binary operator. got boolean AND string. see bool(), int(), float(), string()"),
		keyStruct{true, "NON_BOOL_VALUE", tick.TokenOr}:       errors.New("mismatched type to binary operator. got boolean OR string. see bool(), int(), float(), string()"),

		// Left: False, Right: "NON_BOOL_VALUE"
		keyStruct{false, "NON_BOOL_VALUE", tick.TokenEqual}:    errors.New("mismatched type to binary operator. got boolean == string. see bool(), int(), float(), string()"),
		keyStruct{false, "NON_BOOL_VALUE", tick.TokenNotEqual}: errors.New("mismatched type to binary operator. got boolean != string. see bool(), int(), float(), string()"),
		keyStruct{false, "NON_BOOL_VALUE", tick.TokenAnd}:      errors.New("mismatched type to binary operator. got boolean AND string. see bool(), int(), float(), string()"),
		keyStruct{false, "NON_BOOL_VALUE", tick.TokenOr}:       errors.New("mismatched type to binary operator. got boolean OR string. see bool(), int(), float(), string()"),

		// Left: "NON_BOOL_VALUE", Right: True
		keyStruct{"NON_BOOL_VALUE", true, tick.TokenEqual}:    errors.New("mismatched type to binary operator. got string == bool. see bool(), int(), float(), string()"),
		keyStruct{"NON_BOOL_VALUE", true, tick.TokenNotEqual}: errors.New("mismatched type to binary operator. got string != bool. see bool(), int(), float(), string()"),
		keyStruct{"NON_BOOL_VALUE", true, tick.TokenAnd}:      errors.New("mismatched type to binary operator. got string AND bool. see bool(), int(), float(), string()"),
		keyStruct{"NON_BOOL_VALUE", true, tick.TokenOr}:       errors.New("invalid comparison operator OR for type string"),

		// Left: "NON_BOOL_VALUE", Right: False
		keyStruct{"NON_BOOL_VALUE", false, tick.TokenEqual}:    errors.New("mismatched type to binary operator. got string == bool. see bool(), int(), float(), string()"),
		keyStruct{"NON_BOOL_VALUE", false, tick.TokenNotEqual}: errors.New("mismatched type to binary operator. got string != bool. see bool(), int(), float(), string()"),
		keyStruct{"NON_BOOL_VALUE", false, tick.TokenAnd}:      errors.New("mismatched type to binary operator. got string AND bool. see bool(), int(), float(), string()"),
		keyStruct{"NON_BOOL_VALUE", false, tick.TokenOr}:       errors.New("invalid comparison operator OR for type string"),
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
		keyStruct{float64(5), float64(5), tick.TokenOr}:   errors.New("invalid logical operator OR for type float64"),
		keyStruct{float64(5), float64(10), tick.TokenOr}:  errors.New("invalid logical operator OR for type float64"),
		keyStruct{float64(5), int64(5), tick.TokenOr}:     errors.New("invalid logical operator OR for type float64"),
		keyStruct{float64(10), float64(5), tick.TokenOr}:  errors.New("invalid logical operator OR for type float64"),
		keyStruct{float64(10), float64(10), tick.TokenOr}: errors.New("invalid logical operator OR for type float64"),
		keyStruct{float64(10), int64(5), tick.TokenOr}:    errors.New("invalid logical operator OR for type float64"),
		keyStruct{int64(5), float64(5), tick.TokenOr}:     errors.New("invalid logical operator OR for type int64"),
		keyStruct{int64(5), float64(10), tick.TokenOr}:    errors.New("invalid logical operator OR for type int64"),
		keyStruct{int64(5), int64(5), tick.TokenOr}:       errors.New("invalid logical operator OR for type int64"),

		// (Redundant case)
		keyStruct{float64(5), "NON_INT_VALUE", tick.TokenOr}:  errors.New("invalid logical operator OR for type float64"),
		keyStruct{float64(10), "NON_INT_VALUE", tick.TokenOr}: errors.New("invalid logical operator OR for type float64"),
		keyStruct{int64(5), "NON_INT_VALUE", tick.TokenOr}:    errors.New("invalid logical operator OR for type int64"),

		// Left is float64(5), Right is "NON_INT_VALUE"
		keyStruct{float64(5), "NON_INT_VALUE", tick.TokenEqual}:        errors.New("mismatched type to binary operator. got float64 == string. see bool(), int(), float(), string()"),
		keyStruct{float64(5), "NON_INT_VALUE", tick.TokenNotEqual}:     errors.New("mismatched type to binary operator. got float64 != string. see bool(), int(), float(), string()"),
		keyStruct{float64(5), "NON_INT_VALUE", tick.TokenGreater}:      errors.New("mismatched type to binary operator. got float64 > string. see bool(), int(), float(), string()"),
		keyStruct{float64(5), "NON_INT_VALUE", tick.TokenGreaterEqual}: errors.New("mismatched type to binary operator. got float64 >= string. see bool(), int(), float(), string()"),
		keyStruct{float64(5), "NON_INT_VALUE", tick.TokenLess}:         errors.New("mismatched type to binary operator. got float64 < string. see bool(), int(), float(), string()"),
		keyStruct{float64(5), "NON_INT_VALUE", tick.TokenLessEqual}:    errors.New("mismatched type to binary operator. got float64 <= string. see bool(), int(), float(), string()"),

		// (Redundant case) Left is float64(10), Right is "NON_INT_VALUE"
		keyStruct{float64(10), "NON_INT_VALUE", tick.TokenEqual}:        errors.New("mismatched type to binary operator. got float64 == string. see bool(), int(), float(), string()"),
		keyStruct{float64(10), "NON_INT_VALUE", tick.TokenNotEqual}:     errors.New("mismatched type to binary operator. got float64 != string. see bool(), int(), float(), string()"),
		keyStruct{float64(10), "NON_INT_VALUE", tick.TokenGreater}:      errors.New("mismatched type to binary operator. got float64 > string. see bool(), int(), float(), string()"),
		keyStruct{float64(10), "NON_INT_VALUE", tick.TokenGreaterEqual}: errors.New("mismatched type to binary operator. got float64 >= string. see bool(), int(), float(), string()"),
		keyStruct{float64(10), "NON_INT_VALUE", tick.TokenLess}:         errors.New("mismatched type to binary operator. got float64 < string. see bool(), int(), float(), string()"),
		keyStruct{float64(10), "NON_INT_VALUE", tick.TokenLessEqual}:    errors.New("mismatched type to binary operator. got float64 <= string. see bool(), int(), float(), string()"),

		// Left is int64(5), Right is "NON_INT_VALUE"
		keyStruct{int64(5), "NON_INT_VALUE", tick.TokenEqual}:        errors.New("mismatched type to binary operator. got int64 == string. see bool(), int(), float(), string()"),
		keyStruct{int64(5), "NON_INT_VALUE", tick.TokenNotEqual}:     errors.New("mismatched type to binary operator. got int64 != string. see bool(), int(), float(), string()"),
		keyStruct{int64(5), "NON_INT_VALUE", tick.TokenGreater}:      errors.New("mismatched type to binary operator. got int64 > string. see bool(), int(), float(), string()"),
		keyStruct{int64(5), "NON_INT_VALUE", tick.TokenGreaterEqual}: errors.New("mismatched type to binary operator. got int64 >= string. see bool(), int(), float(), string()"),
		keyStruct{int64(5), "NON_INT_VALUE", tick.TokenLess}:         errors.New("mismatched type to binary operator. got int64 < string. see bool(), int(), float(), string()"),
		keyStruct{int64(5), "NON_INT_VALUE", tick.TokenLessEqual}:    errors.New("mismatched type to binary operator. got int64 <= string. see bool(), int(), float(), string()"),
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
		keyStruct{"a", "a", tick.TokenOr}: errors.New("invalid logical operator OR for type string"),
		keyStruct{"a", "b", tick.TokenOr}: errors.New("invalid logical operator OR for type string"),
		keyStruct{"b", "a", tick.TokenOr}: errors.New("invalid logical operator OR for type string"),
		keyStruct{"b", "b", tick.TokenOr}: errors.New("invalid logical operator OR for type string"),

		keyStruct{"a", int64(123), tick.TokenOr}: errors.New("invalid logical operator OR for type string"),
		keyStruct{"b", int64(123), tick.TokenOr}: errors.New("invalid logical operator OR for type string"),

		// Left is "a", Right is int64(123)
		keyStruct{"a", int64(123), tick.TokenEqual}:        errors.New("mismatched type to binary operator. got string == int64. see bool(), int(), float(), string()"),
		keyStruct{"a", int64(123), tick.TokenNotEqual}:     errors.New("mismatched type to binary operator. got string != int64. see bool(), int(), float(), string()"),
		keyStruct{"a", int64(123), tick.TokenGreater}:      errors.New("mismatched type to binary operator. got string > int64. see bool(), int(), float(), string()"),
		keyStruct{"a", int64(123), tick.TokenGreaterEqual}: errors.New("mismatched type to binary operator. got string >= int64. see bool(), int(), float(), string()"),
		keyStruct{"a", int64(123), tick.TokenLess}:         errors.New("mismatched type to binary operator. got string < int64. see bool(), int(), float(), string()"),
		keyStruct{"a", int64(123), tick.TokenLessEqual}:    errors.New("mismatched type to binary operator. got string <= int64. see bool(), int(), float(), string()"),

		// Left is "b", Right is int64(123)
		keyStruct{"b", int64(123), tick.TokenEqual}:        errors.New("mismatched type to binary operator. got string == int64. see bool(), int(), float(), string()"),
		keyStruct{"b", int64(123), tick.TokenNotEqual}:     errors.New("mismatched type to binary operator. got string != int64. see bool(), int(), float(), string()"),
		keyStruct{"b", int64(123), tick.TokenGreater}:      errors.New("mismatched type to binary operator. got string > int64. see bool(), int(), float(), string()"),
		keyStruct{"b", int64(123), tick.TokenGreaterEqual}: errors.New("mismatched type to binary operator. got string >= int64. see bool(), int(), float(), string()"),
		keyStruct{"b", int64(123), tick.TokenLess}:         errors.New("mismatched type to binary operator. got string < int64. see bool(), int(), float(), string()"),
		keyStruct{"b", int64(123), tick.TokenLessEqual}:    errors.New("mismatched type to binary operator. got string <= int64. see bool(), int(), float(), string()"),
	})
}

func TestExpression_EvalBool_RegexNode(t *testing.T) {
	pattern := regexp.MustCompile(`^(.*)c$`)

	leftValues := []interface{}{"abc", "cba", pattern}

	// Right values are regex, but we are supplying strings because the keyStruct and maps don't play nice together
	// so we mark regex with prefix of regexp.MustCompile(``) and createStringOrRegexNode will convert it to regex
	rightValues := []interface{}{pattern}
	operators := []tick.TokenType{tick.TokenRegexEqual, tick.TokenRegexNotEqual, tick.TokenEqual}

	createStringOrRegexNode := func(v interface{}) tick.Node {
		switch value := v.(type) {
		case string:
			return &tick.StringNode{
				Literal: value,
			}
		case *regexp.Regexp:
			return &tick.RegexNode{
				Regex: value,
			}
		default:
			panic(fmt.Sprintf("unexpected type %T", v))
		}
	}

	runCompiledEvalBoolTests(t, createStringOrRegexNode, leftValues, rightValues, operators, map[keyStruct]interface{}{
		// Left is "abc", Right is regex "(.*)c"
		keyStruct{"abc", pattern, tick.TokenRegexEqual}:    true,
		keyStruct{"abc", pattern, tick.TokenRegexNotEqual}: false,

		// Left is "cba", Right is regex "(.*)c"
		keyStruct{"cba", pattern, tick.TokenRegexEqual}:    false,
		keyStruct{"cba", pattern, tick.TokenRegexNotEqual}: true,
	},
		map[keyStruct]error{
			// Errors for invalid operators
			keyStruct{"abc", pattern, tick.TokenEqual}:           errors.New("mismatched type to binary operator. got string == regex. see bool(), int(), float(), string()"),
			keyStruct{"cba", pattern, tick.TokenEqual}:           errors.New("mismatched type to binary operator. got string == regex. see bool(), int(), float(), string()"),
			keyStruct{pattern, "cba", tick.TokenEqual}:           errors.New("invalid comparison operator == for type regex"),
			keyStruct{pattern, pattern, tick.TokenRegexEqual}:    errors.New("mismatched type to binary operator. got regex =~ regex. see bool(), int(), float(), string()"),
			keyStruct{pattern, pattern, tick.TokenRegexNotEqual}: errors.New("mismatched type to binary operator. got regex !~ regex. see bool(), int(), float(), string()"),
			keyStruct{pattern, pattern, tick.TokenEqual}:         errors.New("invalid comparison operator == for type regex"),
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
	node := &tick.BinaryNode{
		Operator: tick.TokenType(666),
		Left: &tick.StringNode{
			Literal: "value",
		},
		Right: &tick.StringNode{
			Literal: "yo",
		},
	}
	expectedError := "unknown binary operator 666"
	_, err := stateful.NewExpression(node)
	if err == nil {
		t.Fatal("Unexpected error result: but didn't got any error")
	}
	if got := err.Error(); got != expectedError {
		t.Errorf("Unexpected error result: \ngot: %v\nexpected: %v", got, expectedError)
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
	se := mustCompileExpression(&tick.ReferenceNode{
		Reference: "value",
	})

	result, err := se.Eval(emptyScope)

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

func TestExpression_EvalString_StringConcat(t *testing.T) {
	se, err := stateful.NewExpression(&tick.BinaryNode{
		Operator: tick.TokenPlus,
		Left: &tick.StringNode{
			Literal: "left",
		},
		Right: &tick.StringNode{
			Literal: "right",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	scope := tick.NewScope()
	result, err := se.EvalString(scope)
	if err != nil {
		t.Fatal("unexpected error EvalString:", err)
	}
	if exp := "leftright"; exp != result {
		t.Errorf("unexpected EvalString results: got %s exp %s", result, exp)
	}
}

func TestExpression_EvalString_StringConcatReferenceNode(t *testing.T) {
	se, err := stateful.NewExpression(&tick.BinaryNode{
		Operator: tick.TokenPlus,
		Left: &tick.StringNode{
			Literal: "left",
		},
		Right: &tick.ReferenceNode{
			Reference: "value",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	scope := tick.NewScope()
	scope.Set("value", "right")
	result, err := se.EvalString(scope)
	if err != nil {
		t.Fatal("unexpected error EvalString:", err)
	}
	if exp := "leftright"; exp != result {
		t.Errorf("unexpected EvalString results: got %s exp %s", result, exp)
	}
}

func TestExpression_EvalNum_BinaryNodeWithUnary(t *testing.T) {

	// -"value" < 0 , yes, of course, this is always true..
	se := mustCompileExpression(&tick.BinaryNode{
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

	se := mustCompileExpression(&tick.BinaryNode{
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
	se = mustCompileExpression(&tick.BinaryNode{
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

	se := mustCompileExpression(&tick.BinaryNode{
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
	se := mustCompileExpression(&tick.BinaryNode{
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
	se := mustCompileExpression(&tick.BinaryNode{
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
	se := mustCompileExpression(&tick.BinaryNode{
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
		keyStruct{float64(5), float64(5), tick.TokenMod}:       errors.New("invalid math operator % for type float64"),
		keyStruct{float64(5), float64(10), tick.TokenMod}:      errors.New("invalid math operator % for type float64"),
		keyStruct{float64(10), float64(5), tick.TokenMod}:      errors.New("invalid math operator % for type float64"),
		keyStruct{float64(10), float64(10), tick.TokenMod}:     errors.New("invalid math operator % for type float64"),
		keyStruct{float64(5), int64(5), tick.TokenMod}:         errors.New("invalid math operator % for type float64"),
		keyStruct{float64(10), int64(5), tick.TokenMod}:        errors.New("invalid math operator % for type float64"),
		keyStruct{float64(10), "NON_INT_VALUE", tick.TokenMod}: errors.New("invalid math operator % for type float64"),
		keyStruct{float64(5), "NON_INT_VALUE", tick.TokenMod}:  errors.New("invalid math operator % for type float64"),

		// Left is int, right is float
		keyStruct{int64(5), float64(5), tick.TokenPlus}:   errors.New("mismatched type to binary operator. got int64 + float64. see bool(), int(), float(), string()"),
		keyStruct{int64(5), float64(5), tick.TokenMinus}:  errors.New("mismatched type to binary operator. got int64 - float64. see bool(), int(), float(), string()"),
		keyStruct{int64(5), float64(5), tick.TokenMult}:   errors.New("mismatched type to binary operator. got int64 * float64. see bool(), int(), float(), string()"),
		keyStruct{int64(5), float64(5), tick.TokenDiv}:    errors.New("mismatched type to binary operator. got int64 / float64. see bool(), int(), float(), string()"),
		keyStruct{int64(5), float64(5), tick.TokenMod}:    errors.New("mismatched type to binary operator. got int64 % float64. see bool(), int(), float(), string()"),
		keyStruct{int64(5), float64(10), tick.TokenPlus}:  errors.New("mismatched type to binary operator. got int64 + float64. see bool(), int(), float(), string()"),
		keyStruct{int64(5), float64(10), tick.TokenMinus}: errors.New("mismatched type to binary operator. got int64 - float64. see bool(), int(), float(), string()"),
		keyStruct{int64(5), float64(10), tick.TokenMult}:  errors.New("mismatched type to binary operator. got int64 * float64. see bool(), int(), float(), string()"),
		keyStruct{int64(5), float64(10), tick.TokenDiv}:   errors.New("mismatched type to binary operator. got int64 / float64. see bool(), int(), float(), string()"),
		keyStruct{int64(5), float64(10), tick.TokenMod}:   errors.New("mismatched type to binary operator. got int64 % float64. see bool(), int(), float(), string()"),

		// Left is float, right is int
		keyStruct{float64(5), int64(5), tick.TokenPlus}:  errors.New("mismatched type to binary operator. got float64 + int64. see bool(), int(), float(), string()"),
		keyStruct{float64(5), int64(5), tick.TokenMinus}: errors.New("mismatched type to binary operator. got float64 - int64. see bool(), int(), float(), string()"),
		keyStruct{float64(5), int64(5), tick.TokenMult}:  errors.New("mismatched type to binary operator. got float64 * int64. see bool(), int(), float(), string()"),
		keyStruct{float64(5), int64(5), tick.TokenDiv}:   errors.New("mismatched type to binary operator. got float64 / int64. see bool(), int(), float(), string()"),

		keyStruct{float64(10), int64(5), tick.TokenPlus}:  errors.New("mismatched type to binary operator. got float64 + int64. see bool(), int(), float(), string()"),
		keyStruct{float64(10), int64(5), tick.TokenMinus}: errors.New("mismatched type to binary operator. got float64 - int64. see bool(), int(), float(), string()"),
		keyStruct{float64(10), int64(5), tick.TokenMult}:  errors.New("mismatched type to binary operator. got float64 * int64. see bool(), int(), float(), string()"),
		keyStruct{float64(10), int64(5), tick.TokenDiv}:   errors.New("mismatched type to binary operator. got float64 / int64. see bool(), int(), float(), string()"),

		// Left is int64, Right is "NON_INT_VALUE"
		keyStruct{int64(5), "NON_INT_VALUE", tick.TokenPlus}:  errors.New("mismatched type to binary operator. got int64 + string. see bool(), int(), float(), string()"),
		keyStruct{int64(5), "NON_INT_VALUE", tick.TokenMinus}: errors.New("mismatched type to binary operator. got int64 - string. see bool(), int(), float(), string()"),
		keyStruct{int64(5), "NON_INT_VALUE", tick.TokenMult}:  errors.New("mismatched type to binary operator. got int64 * string. see bool(), int(), float(), string()"),
		keyStruct{int64(5), "NON_INT_VALUE", tick.TokenDiv}:   errors.New("mismatched type to binary operator. got int64 / string. see bool(), int(), float(), string()"),
		keyStruct{int64(5), "NON_INT_VALUE", tick.TokenMod}:   errors.New("mismatched type to binary operator. got int64 % string. see bool(), int(), float(), string()"),

		// Left is float64, Right is "NON_INT_VALUE"
		keyStruct{float64(5), "NON_INT_VALUE", tick.TokenPlus}:   errors.New("mismatched type to binary operator. got float64 + string. see bool(), int(), float(), string()"),
		keyStruct{float64(5), "NON_INT_VALUE", tick.TokenMinus}:  errors.New("mismatched type to binary operator. got float64 - string. see bool(), int(), float(), string()"),
		keyStruct{float64(5), "NON_INT_VALUE", tick.TokenMult}:   errors.New("mismatched type to binary operator. got float64 * string. see bool(), int(), float(), string()"),
		keyStruct{float64(5), "NON_INT_VALUE", tick.TokenDiv}:    errors.New("mismatched type to binary operator. got float64 / string. see bool(), int(), float(), string()"),
		keyStruct{float64(10), "NON_INT_VALUE", tick.TokenPlus}:  errors.New("mismatched type to binary operator. got float64 + string. see bool(), int(), float(), string()"),
		keyStruct{float64(10), "NON_INT_VALUE", tick.TokenMinus}: errors.New("mismatched type to binary operator. got float64 - string. see bool(), int(), float(), string()"),
		keyStruct{float64(10), "NON_INT_VALUE", tick.TokenMult}:  errors.New("mismatched type to binary operator. got float64 * string. see bool(), int(), float(), string()"),
		keyStruct{float64(10), "NON_INT_VALUE", tick.TokenDiv}:   errors.New("mismatched type to binary operator. got float64 / string. see bool(), int(), float(), string()"),
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
		se, err := stateful.NewExpression(n)
		if err != nil {
			return nil, err
		}
		return se.Eval(scope)
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
	se, err := stateful.NewExpression(n)
	if err != nil {
		return nil, err
	}
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

				t.Log("testing", lhs, op, rhs)
				key := keyStruct{lhs, rhs, op}
				exp, isExpectedResultOk := expected[key]
				errorExpected, isErrorOk := errorExpectations[key]
				if !isExpectedResultOk && !isErrorOk {
					t.Fatalf("Couldn't find an expected result/error for: lhs: %v, rhs: %v, op: %v", lhs, rhs, op)
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
					t.Errorf("Got an error while evaluating: %v %v %v -- %T %v %T -- %v\n", lhs, op, rhs, lhs, op, rhs, err)
				} else {

					// Expect value can be error or bool
					if isErrorOk && errorExpected.Error() != err.Error() {
						t.Errorf("unexpected error result: %v %v %v\ngot: %v\nexp: %v", lhs, op, rhs, err, errorExpected)
					} else if isExpectedResultOk && exp != result {
						t.Errorf("unexpected result: %v %v %v\ngot: %v\nexp: %v", lhs, op, rhs, result, exp)
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
						t.Errorf("reference test: expected an error but got result: %v %v %v\nresult: %v\nerr: %v", lhs, op, rhs, result, err)
					} else if errorExpected.Error() != err.Error() {
						t.Errorf("reference test: unexpected error result: %v %v %v\ngot: %v\nexp: %v", lhs, op, rhs, err, errorExpected)
					}
				} else if isExpectedResultOk && exp != result {
					t.Errorf("reference test: unexpected bool result: %v %v %v\ngot: %v\nexp: %v", lhs, op, rhs, result, exp)
				}

			}

		}
	}
}

func mustCompileExpression(node tick.Node) stateful.Expression {
	se, err := stateful.NewExpression(node)
	if err != nil {
		panic(fmt.Sprintf("Failed to compile expression: %v", err))
	}

	return se
}
