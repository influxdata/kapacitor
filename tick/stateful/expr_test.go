package stateful_test

import (
	"errors"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/influxdata/kapacitor/tick/ast"
	"github.com/influxdata/kapacitor/tick/stateful"
)

type keyStruct struct {
	lhs interface{}
	rhs interface{}
	op  ast.TokenType
}

func TestExpression_EvalNum_KeepsFunctionsState(t *testing.T) {
	se := mustCompileExpression(&ast.FunctionNode{
		Func: "sigma",
		Args: []ast.Node{&ast.ReferenceNode{Reference: "value"}},
	})

	// first
	scope := stateful.NewScope()
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
	leftValues := []interface{}{time.Duration(5), time.Duration(10)}
	rightValues := []interface{}{time.Duration(5), time.Duration(10), int64(5)}

	operators := []ast.TokenType{ast.TokenEqual, ast.TokenNotEqual, ast.TokenGreater, ast.TokenGreaterEqual, ast.TokenLessEqual, ast.TokenLess, ast.TokenOr}

	createNode := func(v interface{}) ast.Node {
		switch value := v.(type) {
		case time.Duration:
			return &ast.DurationNode{
				Dur: value,
			}
		// For the error case
		case int64:
			return &ast.NumberNode{
				IsInt: true,
				Int64: value,
			}
		default:
			panic(fmt.Sprintf("unexpected type time.Duration/int64: %T", v))
		}
	}

	runCompiledEvalBoolTests(t, createNode, leftValues, rightValues, operators, map[keyStruct]interface{}{
		// Left is float64(5), Right is float64(5)
		keyStruct{time.Duration(5), time.Duration(5), ast.TokenEqual}:        true,
		keyStruct{time.Duration(5), time.Duration(5), ast.TokenNotEqual}:     false,
		keyStruct{time.Duration(5), time.Duration(5), ast.TokenGreater}:      false,
		keyStruct{time.Duration(5), time.Duration(5), ast.TokenGreaterEqual}: true,
		keyStruct{time.Duration(5), time.Duration(5), ast.TokenLess}:         false,
		keyStruct{time.Duration(5), time.Duration(5), ast.TokenLessEqual}:    true,

		// Left is time.Duration(5), Right is time.Duration(10)
		keyStruct{time.Duration(5), time.Duration(10), ast.TokenEqual}:        false,
		keyStruct{time.Duration(5), time.Duration(10), ast.TokenNotEqual}:     true,
		keyStruct{time.Duration(5), time.Duration(10), ast.TokenGreater}:      false,
		keyStruct{time.Duration(5), time.Duration(10), ast.TokenGreaterEqual}: false,
		keyStruct{time.Duration(5), time.Duration(10), ast.TokenLess}:         true,
		keyStruct{time.Duration(5), time.Duration(10), ast.TokenLessEqual}:    true,

		// Left is time.Duration(10), Right is time.Duration(5)
		keyStruct{time.Duration(10), time.Duration(5), ast.TokenEqual}:        false,
		keyStruct{time.Duration(10), time.Duration(5), ast.TokenNotEqual}:     true,
		keyStruct{time.Duration(10), time.Duration(5), ast.TokenGreater}:      true,
		keyStruct{time.Duration(10), time.Duration(5), ast.TokenGreaterEqual}: true,
		keyStruct{time.Duration(10), time.Duration(5), ast.TokenLess}:         false,
		keyStruct{time.Duration(10), time.Duration(5), ast.TokenLessEqual}:    false,

		// Left is time.Duration(10), Right is time.Duration(10)
		keyStruct{time.Duration(10), time.Duration(10), ast.TokenEqual}:        true,
		keyStruct{time.Duration(10), time.Duration(10), ast.TokenNotEqual}:     false,
		keyStruct{time.Duration(10), time.Duration(10), ast.TokenGreater}:      false,
		keyStruct{time.Duration(10), time.Duration(10), ast.TokenGreaterEqual}: true,
		keyStruct{time.Duration(10), time.Duration(10), ast.TokenLess}:         false,
		keyStruct{time.Duration(10), time.Duration(10), ast.TokenLessEqual}:    true,
	}, map[keyStruct]error{
		// Invalid operator
		keyStruct{time.Duration(5), time.Duration(5), ast.TokenOr}:   errors.New("invalid logical operator OR for type duration"),
		keyStruct{time.Duration(5), time.Duration(10), ast.TokenOr}:  errors.New("invalid logical operator OR for type duration"),
		keyStruct{time.Duration(5), int64(5), ast.TokenOr}:           errors.New("invalid logical operator OR for type duration"),
		keyStruct{time.Duration(10), time.Duration(5), ast.TokenOr}:  errors.New("invalid logical operator OR for type duration"),
		keyStruct{time.Duration(10), time.Duration(10), ast.TokenOr}: errors.New("invalid logical operator OR for type duration"),
		keyStruct{time.Duration(10), int64(5), ast.TokenOr}:          errors.New("invalid logical operator OR for type duration"),
		keyStruct{int64(5), time.Duration(5), ast.TokenOr}:           errors.New("invalid logical operator OR for type int"),
		keyStruct{int64(5), time.Duration(10), ast.TokenOr}:          errors.New("invalid logical operator OR for type int"),
		keyStruct{int64(5), int64(5), ast.TokenOr}:                   errors.New("invalid logical operator OR for type int"),

		// (Redundant case)
		keyStruct{time.Duration(5), int64(5), ast.TokenOr}:  errors.New("invalid logical operator OR for type duration"),
		keyStruct{time.Duration(10), int64(5), ast.TokenOr}: errors.New("invalid logical operator OR for type duration"),
		keyStruct{int64(5), int64(5), ast.TokenOr}:          errors.New("invalid logical operator OR for type int"),

		// Left is time.Duration(5), Right is int64(5)
		keyStruct{time.Duration(5), int64(5), ast.TokenEqual}:        errors.New("mismatched type to binary operator. got duration == int. see bool(), int(), float(), string(), duration()"),
		keyStruct{time.Duration(5), int64(5), ast.TokenNotEqual}:     errors.New("mismatched type to binary operator. got duration != int. see bool(), int(), float(), string(), duration()"),
		keyStruct{time.Duration(5), int64(5), ast.TokenGreater}:      errors.New("mismatched type to binary operator. got duration > int. see bool(), int(), float(), string(), duration()"),
		keyStruct{time.Duration(5), int64(5), ast.TokenGreaterEqual}: errors.New("mismatched type to binary operator. got duration >= int. see bool(), int(), float(), string(), duration()"),
		keyStruct{time.Duration(5), int64(5), ast.TokenLess}:         errors.New("mismatched type to binary operator. got duration < int. see bool(), int(), float(), string(), duration()"),
		keyStruct{time.Duration(5), int64(5), ast.TokenLessEqual}:    errors.New("mismatched type to binary operator. got duration <= int. see bool(), int(), float(), string(), duration()"),

		// (Redundant case) Left is time.Duration(10), Right is int64(5)
		keyStruct{time.Duration(10), int64(5), ast.TokenEqual}:        errors.New("mismatched type to binary operator. got duration == int. see bool(), int(), float(), string(), duration()"),
		keyStruct{time.Duration(10), int64(5), ast.TokenNotEqual}:     errors.New("mismatched type to binary operator. got duration != int. see bool(), int(), float(), string(), duration()"),
		keyStruct{time.Duration(10), int64(5), ast.TokenGreater}:      errors.New("mismatched type to binary operator. got duration > int. see bool(), int(), float(), string(), duration()"),
		keyStruct{time.Duration(10), int64(5), ast.TokenGreaterEqual}: errors.New("mismatched type to binary operator. got duration >= int. see bool(), int(), float(), string(), duration()"),
		keyStruct{time.Duration(10), int64(5), ast.TokenLess}:         errors.New("mismatched type to binary operator. got duration < int. see bool(), int(), float(), string(), duration()"),
		keyStruct{time.Duration(10), int64(5), ast.TokenLessEqual}:    errors.New("mismatched type to binary operator. got duration <= int. see bool(), int(), float(), string(), duration()"),
	})

}
func TestExpression_EvalDuration(t *testing.T) {
	leftValues := []interface{}{time.Duration(5), time.Duration(10), int64(2), float64(1.5)}
	rightValues := []interface{}{time.Duration(5), time.Duration(10), int64(2), float64(1.5)}

	operators := []ast.TokenType{
		ast.TokenPlus,
		ast.TokenMinus,
		ast.TokenMult,
		ast.TokenDiv,
		ast.TokenMod,
	}

	createNode := func(v interface{}) ast.Node {
		switch value := v.(type) {
		case time.Duration:
			return &ast.DurationNode{
				Dur: value,
			}
		case float64:
			return &ast.NumberNode{
				IsFloat: true,
				Float64: value,
			}
		case int64:
			return &ast.NumberNode{
				IsInt: true,
				Int64: value,
			}
		default:
			panic(fmt.Sprintf("unexpected type time.Duration/int64: %T", v))
		}
	}

	runCompiledNumericTests(t, createNode, leftValues, rightValues, operators, map[keyStruct]interface{}{
		// Left is time.Duration(5), Right is time.Duration(5)
		keyStruct{time.Duration(5), time.Duration(5), ast.TokenPlus}:  time.Duration(10),
		keyStruct{time.Duration(5), time.Duration(5), ast.TokenMinus}: time.Duration(0),
		keyStruct{time.Duration(5), time.Duration(5), ast.TokenDiv}:   int64(1),

		// Left is time.Duration(5), Right is time.Duration(10)
		keyStruct{time.Duration(5), time.Duration(10), ast.TokenPlus}:  time.Duration(15),
		keyStruct{time.Duration(5), time.Duration(10), ast.TokenMinus}: time.Duration(-5),
		keyStruct{time.Duration(5), time.Duration(10), ast.TokenDiv}:   int64(0),

		// Left is time.Duration(5), Right is int64(2)
		keyStruct{time.Duration(5), int64(2), ast.TokenMult}: time.Duration(10),
		keyStruct{time.Duration(5), int64(2), ast.TokenDiv}:  time.Duration(2),

		// Left is time.Duration(5), Right is float64(1.5)
		keyStruct{time.Duration(5), float64(1.5), ast.TokenMult}: time.Duration(7),
		keyStruct{time.Duration(5), float64(1.5), ast.TokenDiv}:  time.Duration(3),

		// Left is time.Duration(10), Right is time.Duration(5)
		keyStruct{time.Duration(10), time.Duration(5), ast.TokenPlus}:  time.Duration(15),
		keyStruct{time.Duration(10), time.Duration(5), ast.TokenMinus}: time.Duration(5),
		keyStruct{time.Duration(10), time.Duration(5), ast.TokenDiv}:   int64(2),

		// Left is time.Duration(10), Right is time.Duration(10)
		keyStruct{time.Duration(10), time.Duration(10), ast.TokenPlus}:  time.Duration(20),
		keyStruct{time.Duration(10), time.Duration(10), ast.TokenMinus}: time.Duration(0),
		keyStruct{time.Duration(10), time.Duration(10), ast.TokenDiv}:   int64(1),

		// Left is time.Duration(10), Right is int64(2)
		keyStruct{time.Duration(10), int64(2), ast.TokenMult}: time.Duration(20),
		keyStruct{time.Duration(10), int64(2), ast.TokenDiv}:  time.Duration(5),

		// Left is time.Duration(10), Right is float64(1.5)
		keyStruct{time.Duration(10), float64(1.5), ast.TokenMult}: time.Duration(15),
		keyStruct{time.Duration(10), float64(1.5), ast.TokenDiv}:  time.Duration(6),

		// Left is int64(2)
		keyStruct{int64(2), time.Duration(5), ast.TokenMult}:  time.Duration(10),
		keyStruct{int64(2), time.Duration(10), ast.TokenMult}: time.Duration(20),

		// Left is float64(1.5)
		keyStruct{float64(1.5), time.Duration(5), ast.TokenMult}:  time.Duration(7),
		keyStruct{float64(1.5), time.Duration(10), ast.TokenMult}: time.Duration(15),

		// Non duration related but still valid
		keyStruct{int64(2), int64(2), ast.TokenPlus}:          int64(4),
		keyStruct{int64(2), int64(2), ast.TokenMinus}:         int64(0),
		keyStruct{int64(2), int64(2), ast.TokenMult}:          int64(4),
		keyStruct{int64(2), int64(2), ast.TokenDiv}:           int64(1),
		keyStruct{int64(2), int64(2), ast.TokenMod}:           int64(0),
		keyStruct{float64(1.5), float64(1.5), ast.TokenPlus}:  float64(3),
		keyStruct{float64(1.5), float64(1.5), ast.TokenMinus}: float64(0),
		keyStruct{float64(1.5), float64(1.5), ast.TokenMult}:  float64(1.5 * 1.5),
		keyStruct{float64(1.5), float64(1.5), ast.TokenDiv}:   float64(1),
	}, map[keyStruct]error{
		// Modulo token where left is float
		keyStruct{time.Duration(5), time.Duration(5), ast.TokenMod}:   errors.New("invalid math operator % for type duration"),
		keyStruct{time.Duration(5), time.Duration(10), ast.TokenMod}:  errors.New("invalid math operator % for type duration"),
		keyStruct{time.Duration(5), int64(2), ast.TokenMod}:           errors.New("invalid math operator % for type duration"),
		keyStruct{time.Duration(5), float64(1.5), ast.TokenMod}:       errors.New("invalid math operator % for type duration"),
		keyStruct{time.Duration(10), time.Duration(5), ast.TokenMod}:  errors.New("invalid math operator % for type duration"),
		keyStruct{time.Duration(10), time.Duration(10), ast.TokenMod}: errors.New("invalid math operator % for type duration"),
		keyStruct{time.Duration(10), int64(2), ast.TokenMod}:          errors.New("invalid math operator % for type duration"),
		keyStruct{time.Duration(10), float64(1.5), ast.TokenMod}:      errors.New("invalid math operator % for type duration"),

		// Muiltiply Divide durations
		keyStruct{time.Duration(5), time.Duration(5), ast.TokenMult}:   errors.New("mismatched type to binary operator. got duration * duration. see bool(), int(), float(), string(), duration()"),
		keyStruct{time.Duration(5), time.Duration(10), ast.TokenMult}:  errors.New("mismatched type to binary operator. got duration * duration. see bool(), int(), float(), string(), duration()"),
		keyStruct{time.Duration(10), time.Duration(10), ast.TokenMult}: errors.New("mismatched type to binary operator. got duration * duration. see bool(), int(), float(), string(), duration()"),
		keyStruct{time.Duration(10), time.Duration(5), ast.TokenMult}:  errors.New("mismatched type to binary operator. got duration * duration. see bool(), int(), float(), string(), duration()"),

		// Add/Subtract duration with int/float
		keyStruct{time.Duration(5), int64(2), ast.TokenPlus}:       errors.New("mismatched type to binary operator. got duration + int. see bool(), int(), float(), string(), duration()"),
		keyStruct{time.Duration(5), int64(2), ast.TokenMinus}:      errors.New("mismatched type to binary operator. got duration - int. see bool(), int(), float(), string(), duration()"),
		keyStruct{time.Duration(5), float64(1.5), ast.TokenPlus}:   errors.New("mismatched type to binary operator. got duration + float. see bool(), int(), float(), string(), duration()"),
		keyStruct{time.Duration(5), float64(1.5), ast.TokenMinus}:  errors.New("mismatched type to binary operator. got duration - float. see bool(), int(), float(), string(), duration()"),
		keyStruct{int64(2), time.Duration(5), ast.TokenPlus}:       errors.New("mismatched type to binary operator. got int + duration. see bool(), int(), float(), string(), duration()"),
		keyStruct{int64(2), time.Duration(5), ast.TokenMinus}:      errors.New("mismatched type to binary operator. got int - duration. see bool(), int(), float(), string(), duration()"),
		keyStruct{int64(2), time.Duration(10), ast.TokenPlus}:      errors.New("mismatched type to binary operator. got int + duration. see bool(), int(), float(), string(), duration()"),
		keyStruct{int64(2), time.Duration(10), ast.TokenMinus}:     errors.New("mismatched type to binary operator. got int - duration. see bool(), int(), float(), string(), duration()"),
		keyStruct{float64(1.5), time.Duration(5), ast.TokenPlus}:   errors.New("mismatched type to binary operator. got float + duration. see bool(), int(), float(), string(), duration()"),
		keyStruct{float64(1.5), time.Duration(5), ast.TokenMinus}:  errors.New("mismatched type to binary operator. got float - duration. see bool(), int(), float(), string(), duration()"),
		keyStruct{float64(1.5), time.Duration(10), ast.TokenPlus}:  errors.New("mismatched type to binary operator. got float + duration. see bool(), int(), float(), string(), duration()"),
		keyStruct{float64(1.5), time.Duration(10), ast.TokenMinus}: errors.New("mismatched type to binary operator. got float - duration. see bool(), int(), float(), string(), duration()"),

		// int / duration
		keyStruct{int64(2), time.Duration(5), ast.TokenDiv}:      errors.New("mismatched type to binary operator. got int / duration. see bool(), int(), float(), string(), duration()"),
		keyStruct{int64(2), time.Duration(10), ast.TokenDiv}:     errors.New("mismatched type to binary operator. got int / duration. see bool(), int(), float(), string(), duration()"),
		keyStruct{float64(1.5), time.Duration(5), ast.TokenDiv}:  errors.New("mismatched type to binary operator. got float / duration. see bool(), int(), float(), string(), duration()"),
		keyStruct{float64(1.5), time.Duration(10), ast.TokenDiv}: errors.New("mismatched type to binary operator. got float / duration. see bool(), int(), float(), string(), duration()"),

		// int % duration
		keyStruct{int64(2), time.Duration(5), ast.TokenMod}:      errors.New("mismatched type to binary operator. got int % duration. see bool(), int(), float(), string(), duration()"),
		keyStruct{int64(2), time.Duration(10), ast.TokenMod}:     errors.New("mismatched type to binary operator. got int % duration. see bool(), int(), float(), string(), duration()"),
		keyStruct{float64(1.5), time.Duration(5), ast.TokenMod}:  errors.New("invalid math operator % for type float"),
		keyStruct{float64(1.5), time.Duration(10), ast.TokenMod}: errors.New("invalid math operator % for type float"),

		keyStruct{time.Duration(10), int64(2), ast.TokenPlus}:      errors.New("mismatched type to binary operator. got duration + int. see bool(), int(), float(), string(), duration()"),
		keyStruct{time.Duration(10), int64(2), ast.TokenMinus}:     errors.New("mismatched type to binary operator. got duration - int. see bool(), int(), float(), string(), duration()"),
		keyStruct{time.Duration(10), float64(1.5), ast.TokenPlus}:  errors.New("mismatched type to binary operator. got duration + float. see bool(), int(), float(), string(), duration()"),
		keyStruct{time.Duration(10), float64(1.5), ast.TokenMinus}: errors.New("mismatched type to binary operator. got duration - float. see bool(), int(), float(), string(), duration()"),

		// unrelated to durations but
		keyStruct{int64(2), float64(1.5), ast.TokenPlus}:    errors.New("mismatched type to binary operator. got int + float. see bool(), int(), float(), string(), duration()"),
		keyStruct{int64(2), float64(1.5), ast.TokenMinus}:   errors.New("mismatched type to binary operator. got int - float. see bool(), int(), float(), string(), duration()"),
		keyStruct{int64(2), float64(1.5), ast.TokenMult}:    errors.New("mismatched type to binary operator. got int * float. see bool(), int(), float(), string(), duration()"),
		keyStruct{int64(2), float64(1.5), ast.TokenDiv}:     errors.New("mismatched type to binary operator. got int / float. see bool(), int(), float(), string(), duration()"),
		keyStruct{int64(2), float64(1.5), ast.TokenMod}:     errors.New("mismatched type to binary operator. got int % float. see bool(), int(), float(), string(), duration()"),
		keyStruct{float64(1.5), int64(2), ast.TokenPlus}:    errors.New("mismatched type to binary operator. got float + int. see bool(), int(), float(), string(), duration()"),
		keyStruct{float64(1.5), int64(2), ast.TokenMinus}:   errors.New("mismatched type to binary operator. got float - int. see bool(), int(), float(), string(), duration()"),
		keyStruct{float64(1.5), int64(2), ast.TokenMult}:    errors.New("mismatched type to binary operator. got float * int. see bool(), int(), float(), string(), duration()"),
		keyStruct{float64(1.5), int64(2), ast.TokenDiv}:     errors.New("mismatched type to binary operator. got float / int. see bool(), int(), float(), string(), duration()"),
		keyStruct{float64(1.5), int64(2), ast.TokenMod}:     errors.New("invalid math operator % for type float"),
		keyStruct{float64(1.5), float64(1.5), ast.TokenMod}: errors.New("invalid math operator % for type float"),
	})
}

func TestExpression_Eval_NotSupportedNode(t *testing.T) {
	// Passing IdentifierNode, yeah.. this crazy test, but we want to make sure
	// we don't have panics or crashes
	se, err := stateful.NewExpression(&ast.IdentifierNode{})
	expectedError := errors.New("Given node type is not valid evaluation node: *ast.IdentifierNode")
	if err == nil {
		t.Errorf("EvalBool: Expected error, but got expression: %v", se)
	}

	if err != nil && err.Error() != expectedError.Error() {
		t.Errorf("EvalBool: Got unexpected error:\nexpected: %v\ngot: %v", expectedError, err)
	}

	// BinaryNode - Left is identifier
	se, err = stateful.NewExpression(&ast.BinaryNode{
		Operator: ast.TokenEqual,
		Left:     &ast.IdentifierNode{},
		Right:    &ast.BoolNode{Bool: true},
	})

	expectedError = errors.New("Failed to handle left node: Given node type is not valid evaluation node: *ast.IdentifierNode")
	if err == nil {
		t.Errorf("EvalBool BinaryNode(Left=>Identifier): Expected error, but got expression: %v", se)
	}

	if err != nil && err.Error() != expectedError.Error() {
		t.Errorf("EvalBool BinaryNode(Left=>Identifier): Got unexpected error:\nexpected: %v\ngot: %v", expectedError, err)
	}

	// BinaryNode - Right is identifier
	se, err = stateful.NewExpression(&ast.BinaryNode{
		Operator: ast.TokenEqual,
		Left:     &ast.BoolNode{Bool: true},
		Right:    &ast.IdentifierNode{},
	})

	expectedError = errors.New("Failed to handle right node: Given node type is not valid evaluation node: *ast.IdentifierNode")
	if err == nil {
		t.Errorf("EvalBool BinaryNode(Right=>Identifier): Expected error, but got expression: %v", se)
	}

	if err != nil && err.Error() != expectedError.Error() {
		t.Errorf("EvalBool BinaryNode(Right=>Identifier): Got unexpected error:\nexpected: %v\ngot: %v", expectedError, err)
	}
}

func TestExpression_Eval_NodeAndEvalTypeNotMatching(t *testing.T) {
	// Test EvalBool against BinaryNode that returns math result
	se := mustCompileExpression(&ast.BinaryNode{
		Operator: ast.TokenPlus,
		Left: &ast.NumberNode{
			IsFloat: true,
			Float64: float64(5),
		},
		Right: &ast.NumberNode{
			IsFloat: true,
			Float64: float64(10),
		},
	})

	result, err := se.EvalBool(stateful.NewScope())
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
	operators := []ast.TokenType{ast.TokenEqual, ast.TokenNotEqual, ast.TokenAnd, ast.TokenOr, ast.TokenLess}

	createBoolNode := func(v interface{}) ast.Node {
		switch value := v.(type) {
		case bool:
			return &ast.BoolNode{
				Bool: value,
			}
		case string:
			return &ast.StringNode{
				Literal: value,
			}
		default:
			panic(fmt.Sprintf("unexpected type %T", v))
		}
	}

	runCompiledEvalBoolTests(t, createBoolNode, leftValues, rightValues, operators, map[keyStruct]interface{}{
		// Left: True, Right: True
		keyStruct{true, true, ast.TokenEqual}:    true,
		keyStruct{true, true, ast.TokenNotEqual}: false,
		keyStruct{true, true, ast.TokenAnd}:      true,
		keyStruct{true, true, ast.TokenOr}:       true,

		// Left: True, Right: False
		keyStruct{true, false, ast.TokenEqual}:    false,
		keyStruct{true, false, ast.TokenNotEqual}: true,
		keyStruct{true, false, ast.TokenAnd}:      false,
		keyStruct{true, false, ast.TokenOr}:       true,

		// Left: False, Right: True
		keyStruct{false, true, ast.TokenEqual}:    false,
		keyStruct{false, true, ast.TokenNotEqual}: true,
		keyStruct{false, true, ast.TokenAnd}:      false,
		keyStruct{false, true, ast.TokenOr}:       true,

		// Left: False, Right: False
		keyStruct{false, false, ast.TokenEqual}:    true,
		keyStruct{false, false, ast.TokenNotEqual}: false,
		keyStruct{false, false, ast.TokenAnd}:      false,
		keyStruct{false, false, ast.TokenOr}:       false,
	}, map[keyStruct]error{
		// Check invalid bool operator
		keyStruct{true, true, ast.TokenLess}:   errors.New("invalid comparison operator < for type boolean"),
		keyStruct{true, false, ast.TokenLess}:  errors.New("invalid comparison operator < for type boolean"),
		keyStruct{false, true, ast.TokenLess}:  errors.New("invalid comparison operator < for type boolean"),
		keyStruct{false, false, ast.TokenLess}: errors.New("invalid comparison operator < for type boolean"),

		// (Redundant test case)
		keyStruct{true, "NON_BOOL_VALUE", ast.TokenLess}:  errors.New("invalid comparison operator < for type boolean"),
		keyStruct{false, "NON_BOOL_VALUE", ast.TokenLess}: errors.New("invalid comparison operator < for type boolean"),

		// Left: True, Right: "NON_BOOL_VALUE"
		keyStruct{true, "NON_BOOL_VALUE", ast.TokenEqual}:    errors.New("mismatched type to binary operator. got boolean == string. see bool(), int(), float(), string(), duration()"),
		keyStruct{true, "NON_BOOL_VALUE", ast.TokenNotEqual}: errors.New("mismatched type to binary operator. got boolean != string. see bool(), int(), float(), string(), duration()"),
		keyStruct{true, "NON_BOOL_VALUE", ast.TokenAnd}:      errors.New("mismatched type to binary operator. got boolean AND string. see bool(), int(), float(), string(), duration()"),
		keyStruct{true, "NON_BOOL_VALUE", ast.TokenOr}:       errors.New("mismatched type to binary operator. got boolean OR string. see bool(), int(), float(), string(), duration()"),

		// Left: False, Right: "NON_BOOL_VALUE"
		keyStruct{false, "NON_BOOL_VALUE", ast.TokenEqual}:    errors.New("mismatched type to binary operator. got boolean == string. see bool(), int(), float(), string(), duration()"),
		keyStruct{false, "NON_BOOL_VALUE", ast.TokenNotEqual}: errors.New("mismatched type to binary operator. got boolean != string. see bool(), int(), float(), string(), duration()"),
		keyStruct{false, "NON_BOOL_VALUE", ast.TokenAnd}:      errors.New("mismatched type to binary operator. got boolean AND string. see bool(), int(), float(), string(), duration()"),
		keyStruct{false, "NON_BOOL_VALUE", ast.TokenOr}:       errors.New("mismatched type to binary operator. got boolean OR string. see bool(), int(), float(), string(), duration()"),

		// Left: "NON_BOOL_VALUE", Right: True
		keyStruct{"NON_BOOL_VALUE", true, ast.TokenEqual}:    errors.New("mismatched type to binary operator. got string == bool. see bool(), int(), float(), string(), duration()"),
		keyStruct{"NON_BOOL_VALUE", true, ast.TokenNotEqual}: errors.New("mismatched type to binary operator. got string != bool. see bool(), int(), float(), string(), duration()"),
		keyStruct{"NON_BOOL_VALUE", true, ast.TokenAnd}:      errors.New("mismatched type to binary operator. got string AND bool. see bool(), int(), float(), string(), duration()"),
		keyStruct{"NON_BOOL_VALUE", true, ast.TokenOr}:       errors.New("invalid comparison operator OR for type string"),

		// Left: "NON_BOOL_VALUE", Right: False
		keyStruct{"NON_BOOL_VALUE", false, ast.TokenEqual}:    errors.New("mismatched type to binary operator. got string == bool. see bool(), int(), float(), string(), duration()"),
		keyStruct{"NON_BOOL_VALUE", false, ast.TokenNotEqual}: errors.New("mismatched type to binary operator. got string != bool. see bool(), int(), float(), string(), duration()"),
		keyStruct{"NON_BOOL_VALUE", false, ast.TokenAnd}:      errors.New("mismatched type to binary operator. got string AND bool. see bool(), int(), float(), string(), duration()"),
		keyStruct{"NON_BOOL_VALUE", false, ast.TokenOr}:       errors.New("invalid comparison operator OR for type string"),
	})

}

func TestExpression_EvalBool_NumberNode(t *testing.T) {
	leftValues := []interface{}{float64(5), float64(10), int64(5)}
	rightValues := []interface{}{float64(5), float64(10), int64(5), "NON_INT_VALUE"}

	operators := []ast.TokenType{ast.TokenEqual, ast.TokenNotEqual, ast.TokenGreater, ast.TokenGreaterEqual, ast.TokenLessEqual, ast.TokenLess, ast.TokenOr}

	createNumberNode := func(v interface{}) ast.Node {
		switch value := v.(type) {
		case float64:
			return &ast.NumberNode{
				IsFloat: true,
				Float64: value,
			}
		case int64:
			return &ast.NumberNode{
				IsInt: true,
				Int64: value,
			}
		// For the error case
		case string:
			return &ast.StringNode{
				Literal: value,
			}
		default:
			t.Fatalf("value supplied to createNumberNode is not string/int/float64: %t", v)
			return nil
		}
	}

	runCompiledEvalBoolTests(t, createNumberNode, leftValues, rightValues, operators, map[keyStruct]interface{}{
		// Left is float64(5), Right is float64(5)
		keyStruct{float64(5), float64(5), ast.TokenEqual}:        true,
		keyStruct{float64(5), float64(5), ast.TokenNotEqual}:     false,
		keyStruct{float64(5), float64(5), ast.TokenGreater}:      false,
		keyStruct{float64(5), float64(5), ast.TokenGreaterEqual}: true,
		keyStruct{float64(5), float64(5), ast.TokenLess}:         false,
		keyStruct{float64(5), float64(5), ast.TokenLessEqual}:    true,

		// Left is float64(5), Right is float64(10)
		keyStruct{float64(5), float64(10), ast.TokenEqual}:        false,
		keyStruct{float64(5), float64(10), ast.TokenNotEqual}:     true,
		keyStruct{float64(5), float64(10), ast.TokenGreater}:      false,
		keyStruct{float64(5), float64(10), ast.TokenGreaterEqual}: false,
		keyStruct{float64(5), float64(10), ast.TokenLess}:         true,
		keyStruct{float64(5), float64(10), ast.TokenLessEqual}:    true,

		// Left is float64(5), Right is int64(5)
		keyStruct{float64(5), int64(5), ast.TokenEqual}:        true,
		keyStruct{float64(5), int64(5), ast.TokenNotEqual}:     false,
		keyStruct{float64(5), int64(5), ast.TokenGreater}:      false,
		keyStruct{float64(5), int64(5), ast.TokenGreaterEqual}: true,
		keyStruct{float64(5), int64(5), ast.TokenLess}:         false,
		keyStruct{float64(5), int64(5), ast.TokenLessEqual}:    true,

		// Left is float64(10), Right is float64(5)
		keyStruct{float64(10), float64(5), ast.TokenEqual}:        false,
		keyStruct{float64(10), float64(5), ast.TokenNotEqual}:     true,
		keyStruct{float64(10), float64(5), ast.TokenGreater}:      true,
		keyStruct{float64(10), float64(5), ast.TokenGreaterEqual}: true,
		keyStruct{float64(10), float64(5), ast.TokenLess}:         false,
		keyStruct{float64(10), float64(5), ast.TokenLessEqual}:    false,

		// Left is float64(10), Right is float64(10)
		keyStruct{float64(10), float64(10), ast.TokenEqual}:        true,
		keyStruct{float64(10), float64(10), ast.TokenNotEqual}:     false,
		keyStruct{float64(10), float64(10), ast.TokenGreater}:      false,
		keyStruct{float64(10), float64(10), ast.TokenGreaterEqual}: true,
		keyStruct{float64(10), float64(10), ast.TokenLess}:         false,
		keyStruct{float64(10), float64(10), ast.TokenLessEqual}:    true,

		// Left is float64(10), Right is float64(5)
		keyStruct{float64(10), int64(5), ast.TokenEqual}:        false,
		keyStruct{float64(10), int64(5), ast.TokenNotEqual}:     true,
		keyStruct{float64(10), int64(5), ast.TokenGreater}:      true,
		keyStruct{float64(10), int64(5), ast.TokenGreaterEqual}: true,
		keyStruct{float64(10), int64(5), ast.TokenLess}:         false,
		keyStruct{float64(10), int64(5), ast.TokenLessEqual}:    false,

		// Left is int64(10), Right is float64(5)
		keyStruct{int64(10), float64(5), ast.TokenEqual}:        false,
		keyStruct{int64(10), float64(5), ast.TokenNotEqual}:     true,
		keyStruct{int64(10), float64(5), ast.TokenGreater}:      true,
		keyStruct{int64(10), float64(5), ast.TokenGreaterEqual}: true,
		keyStruct{int64(10), float64(5), ast.TokenLess}:         false,
		keyStruct{int64(10), float64(5), ast.TokenLessEqual}:    false,

		// Left is int64(5), Right is float64(5)
		keyStruct{int64(5), float64(5), ast.TokenEqual}:        true,
		keyStruct{int64(5), float64(5), ast.TokenNotEqual}:     false,
		keyStruct{int64(5), float64(5), ast.TokenGreater}:      false,
		keyStruct{int64(5), float64(5), ast.TokenGreaterEqual}: true,
		keyStruct{int64(5), float64(5), ast.TokenLess}:         false,
		keyStruct{int64(5), float64(5), ast.TokenLessEqual}:    true,

		// Left is int64(5), Right is float64(10)
		keyStruct{int64(5), float64(10), ast.TokenEqual}:        false,
		keyStruct{int64(5), float64(10), ast.TokenNotEqual}:     true,
		keyStruct{int64(5), float64(10), ast.TokenGreater}:      false,
		keyStruct{int64(5), float64(10), ast.TokenGreaterEqual}: false,
		keyStruct{int64(5), float64(10), ast.TokenLess}:         true,
		keyStruct{int64(5), float64(10), ast.TokenLessEqual}:    true,

		// Left is int64(5), Right is int64(5)
		keyStruct{int64(5), int64(5), ast.TokenEqual}:        true,
		keyStruct{int64(5), int64(5), ast.TokenNotEqual}:     false,
		keyStruct{int64(5), int64(5), ast.TokenGreater}:      false,
		keyStruct{int64(5), int64(5), ast.TokenGreaterEqual}: true,
		keyStruct{int64(5), int64(5), ast.TokenLess}:         false,
		keyStruct{int64(5), int64(5), ast.TokenLessEqual}:    true,
	}, map[keyStruct]error{
		// Invalid operator
		keyStruct{float64(5), float64(5), ast.TokenOr}:   errors.New("invalid logical operator OR for type float"),
		keyStruct{float64(5), float64(10), ast.TokenOr}:  errors.New("invalid logical operator OR for type float"),
		keyStruct{float64(5), int64(5), ast.TokenOr}:     errors.New("invalid logical operator OR for type float"),
		keyStruct{float64(10), float64(5), ast.TokenOr}:  errors.New("invalid logical operator OR for type float"),
		keyStruct{float64(10), float64(10), ast.TokenOr}: errors.New("invalid logical operator OR for type float"),
		keyStruct{float64(10), int64(5), ast.TokenOr}:    errors.New("invalid logical operator OR for type float"),
		keyStruct{int64(5), float64(5), ast.TokenOr}:     errors.New("invalid logical operator OR for type int"),
		keyStruct{int64(5), float64(10), ast.TokenOr}:    errors.New("invalid logical operator OR for type int"),
		keyStruct{int64(5), int64(5), ast.TokenOr}:       errors.New("invalid logical operator OR for type int"),

		// (Redundant case)
		keyStruct{float64(5), "NON_INT_VALUE", ast.TokenOr}:  errors.New("invalid logical operator OR for type float"),
		keyStruct{float64(10), "NON_INT_VALUE", ast.TokenOr}: errors.New("invalid logical operator OR for type float"),
		keyStruct{int64(5), "NON_INT_VALUE", ast.TokenOr}:    errors.New("invalid logical operator OR for type int"),

		// Left is float64(5), Right is "NON_INT_VALUE"
		keyStruct{float64(5), "NON_INT_VALUE", ast.TokenEqual}:        errors.New("mismatched type to binary operator. got float == string. see bool(), int(), float(), string(), duration()"),
		keyStruct{float64(5), "NON_INT_VALUE", ast.TokenNotEqual}:     errors.New("mismatched type to binary operator. got float != string. see bool(), int(), float(), string(), duration()"),
		keyStruct{float64(5), "NON_INT_VALUE", ast.TokenGreater}:      errors.New("mismatched type to binary operator. got float > string. see bool(), int(), float(), string(), duration()"),
		keyStruct{float64(5), "NON_INT_VALUE", ast.TokenGreaterEqual}: errors.New("mismatched type to binary operator. got float >= string. see bool(), int(), float(), string(), duration()"),
		keyStruct{float64(5), "NON_INT_VALUE", ast.TokenLess}:         errors.New("mismatched type to binary operator. got float < string. see bool(), int(), float(), string(), duration()"),
		keyStruct{float64(5), "NON_INT_VALUE", ast.TokenLessEqual}:    errors.New("mismatched type to binary operator. got float <= string. see bool(), int(), float(), string(), duration()"),

		// (Redundant case) Left is float64(10), Right is "NON_INT_VALUE"
		keyStruct{float64(10), "NON_INT_VALUE", ast.TokenEqual}:        errors.New("mismatched type to binary operator. got float == string. see bool(), int(), float(), string(), duration()"),
		keyStruct{float64(10), "NON_INT_VALUE", ast.TokenNotEqual}:     errors.New("mismatched type to binary operator. got float != string. see bool(), int(), float(), string(), duration()"),
		keyStruct{float64(10), "NON_INT_VALUE", ast.TokenGreater}:      errors.New("mismatched type to binary operator. got float > string. see bool(), int(), float(), string(), duration()"),
		keyStruct{float64(10), "NON_INT_VALUE", ast.TokenGreaterEqual}: errors.New("mismatched type to binary operator. got float >= string. see bool(), int(), float(), string(), duration()"),
		keyStruct{float64(10), "NON_INT_VALUE", ast.TokenLess}:         errors.New("mismatched type to binary operator. got float < string. see bool(), int(), float(), string(), duration()"),
		keyStruct{float64(10), "NON_INT_VALUE", ast.TokenLessEqual}:    errors.New("mismatched type to binary operator. got float <= string. see bool(), int(), float(), string(), duration()"),

		// Left is int64(5), Right is "NON_INT_VALUE"
		keyStruct{int64(5), "NON_INT_VALUE", ast.TokenEqual}:        errors.New("mismatched type to binary operator. got int == string. see bool(), int(), float(), string(), duration()"),
		keyStruct{int64(5), "NON_INT_VALUE", ast.TokenNotEqual}:     errors.New("mismatched type to binary operator. got int != string. see bool(), int(), float(), string(), duration()"),
		keyStruct{int64(5), "NON_INT_VALUE", ast.TokenGreater}:      errors.New("mismatched type to binary operator. got int > string. see bool(), int(), float(), string(), duration()"),
		keyStruct{int64(5), "NON_INT_VALUE", ast.TokenGreaterEqual}: errors.New("mismatched type to binary operator. got int >= string. see bool(), int(), float(), string(), duration()"),
		keyStruct{int64(5), "NON_INT_VALUE", ast.TokenLess}:         errors.New("mismatched type to binary operator. got int < string. see bool(), int(), float(), string(), duration()"),
		keyStruct{int64(5), "NON_INT_VALUE", ast.TokenLessEqual}:    errors.New("mismatched type to binary operator. got int <= string. see bool(), int(), float(), string(), duration()"),
	})
}

func TestExpression_EvalBool_StringNode(t *testing.T) {
	leftValues := []interface{}{"a", "b"}
	rightValues := []interface{}{"a", "b", int64(123)}
	operators := []ast.TokenType{ast.TokenEqual, ast.TokenNotEqual, ast.TokenGreater, ast.TokenGreaterEqual, ast.TokenLessEqual, ast.TokenLess, ast.TokenOr}

	createStringNode := func(v interface{}) ast.Node {
		switch value := v.(type) {
		case string:
			return &ast.StringNode{
				Literal: value,
			}
		case int64:
			return &ast.NumberNode{
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
		keyStruct{"a", "a", ast.TokenEqual}:        true,
		keyStruct{"a", "a", ast.TokenNotEqual}:     false,
		keyStruct{"a", "a", ast.TokenGreater}:      false,
		keyStruct{"a", "a", ast.TokenGreaterEqual}: true,
		keyStruct{"a", "a", ast.TokenLess}:         false,
		keyStruct{"a", "a", ast.TokenLessEqual}:    true,

		// Left is "a", Right is "b"
		keyStruct{"a", "b", ast.TokenEqual}:        false,
		keyStruct{"a", "b", ast.TokenNotEqual}:     true,
		keyStruct{"a", "b", ast.TokenGreater}:      false,
		keyStruct{"a", "b", ast.TokenGreaterEqual}: false,
		keyStruct{"a", "b", ast.TokenLess}:         true,
		keyStruct{"a", "b", ast.TokenLessEqual}:    true,

		// Left is "b", Right is "a"
		keyStruct{"b", "a", ast.TokenEqual}:        false,
		keyStruct{"b", "a", ast.TokenNotEqual}:     true,
		keyStruct{"b", "a", ast.TokenGreater}:      true,
		keyStruct{"b", "a", ast.TokenGreaterEqual}: true,
		keyStruct{"b", "a", ast.TokenLess}:         false,
		keyStruct{"b", "a", ast.TokenLessEqual}:    false,

		// Left is "b", Right is "b"
		keyStruct{"b", "b", ast.TokenEqual}:        true,
		keyStruct{"b", "b", ast.TokenNotEqual}:     false,
		keyStruct{"b", "b", ast.TokenGreater}:      false,
		keyStruct{"b", "b", ast.TokenGreaterEqual}: true,
		keyStruct{"b", "b", ast.TokenLess}:         false,
		keyStruct{"b", "b", ast.TokenLessEqual}:    true,
	}, map[keyStruct]error{
		// Invalid operator
		keyStruct{"a", "a", ast.TokenOr}: errors.New("invalid logical operator OR for type string"),
		keyStruct{"a", "b", ast.TokenOr}: errors.New("invalid logical operator OR for type string"),
		keyStruct{"b", "a", ast.TokenOr}: errors.New("invalid logical operator OR for type string"),
		keyStruct{"b", "b", ast.TokenOr}: errors.New("invalid logical operator OR for type string"),

		keyStruct{"a", int64(123), ast.TokenOr}: errors.New("invalid logical operator OR for type string"),
		keyStruct{"b", int64(123), ast.TokenOr}: errors.New("invalid logical operator OR for type string"),

		// Left is "a", Right is int64(123)
		keyStruct{"a", int64(123), ast.TokenEqual}:        errors.New("mismatched type to binary operator. got string == int. see bool(), int(), float(), string(), duration()"),
		keyStruct{"a", int64(123), ast.TokenNotEqual}:     errors.New("mismatched type to binary operator. got string != int. see bool(), int(), float(), string(), duration()"),
		keyStruct{"a", int64(123), ast.TokenGreater}:      errors.New("mismatched type to binary operator. got string > int. see bool(), int(), float(), string(), duration()"),
		keyStruct{"a", int64(123), ast.TokenGreaterEqual}: errors.New("mismatched type to binary operator. got string >= int. see bool(), int(), float(), string(), duration()"),
		keyStruct{"a", int64(123), ast.TokenLess}:         errors.New("mismatched type to binary operator. got string < int. see bool(), int(), float(), string(), duration()"),
		keyStruct{"a", int64(123), ast.TokenLessEqual}:    errors.New("mismatched type to binary operator. got string <= int. see bool(), int(), float(), string(), duration()"),

		// Left is "b", Right is int64(123)
		keyStruct{"b", int64(123), ast.TokenEqual}:        errors.New("mismatched type to binary operator. got string == int. see bool(), int(), float(), string(), duration()"),
		keyStruct{"b", int64(123), ast.TokenNotEqual}:     errors.New("mismatched type to binary operator. got string != int. see bool(), int(), float(), string(), duration()"),
		keyStruct{"b", int64(123), ast.TokenGreater}:      errors.New("mismatched type to binary operator. got string > int. see bool(), int(), float(), string(), duration()"),
		keyStruct{"b", int64(123), ast.TokenGreaterEqual}: errors.New("mismatched type to binary operator. got string >= int. see bool(), int(), float(), string(), duration()"),
		keyStruct{"b", int64(123), ast.TokenLess}:         errors.New("mismatched type to binary operator. got string < int. see bool(), int(), float(), string(), duration()"),
		keyStruct{"b", int64(123), ast.TokenLessEqual}:    errors.New("mismatched type to binary operator. got string <= int. see bool(), int(), float(), string(), duration()"),
	})
}

func TestExpression_EvalBool_RegexNode(t *testing.T) {
	pattern := regexp.MustCompile(`^(.*)c$`)

	leftValues := []interface{}{"abc", "cba", pattern}

	// Right values are regex, but we are supplying strings because the keyStruct and maps don't play nice together
	// so we mark regex with prefix of regexp.MustCompile(``) and createStringOrRegexNode will convert it to regex
	rightValues := []interface{}{pattern}
	operators := []ast.TokenType{ast.TokenRegexEqual, ast.TokenRegexNotEqual, ast.TokenEqual}

	createStringOrRegexNode := func(v interface{}) ast.Node {
		switch value := v.(type) {
		case string:
			return &ast.StringNode{
				Literal: value,
			}
		case *regexp.Regexp:
			return &ast.RegexNode{
				Regex: value,
			}
		default:
			panic(fmt.Sprintf("unexpected type %T", v))
		}
	}

	runCompiledEvalBoolTests(t, createStringOrRegexNode, leftValues, rightValues, operators, map[keyStruct]interface{}{
		// Left is "abc", Right is regex "(.*)c"
		keyStruct{"abc", pattern, ast.TokenRegexEqual}:    true,
		keyStruct{"abc", pattern, ast.TokenRegexNotEqual}: false,

		// Left is "cba", Right is regex "(.*)c"
		keyStruct{"cba", pattern, ast.TokenRegexEqual}:    false,
		keyStruct{"cba", pattern, ast.TokenRegexNotEqual}: true,
	},
		map[keyStruct]error{
			// Errors for invalid operators
			keyStruct{"abc", pattern, ast.TokenEqual}:           errors.New("mismatched type to binary operator. got string == regex. see bool(), int(), float(), string(), duration()"),
			keyStruct{"cba", pattern, ast.TokenEqual}:           errors.New("mismatched type to binary operator. got string == regex. see bool(), int(), float(), string(), duration()"),
			keyStruct{pattern, "cba", ast.TokenEqual}:           errors.New("invalid comparison operator == for type regex"),
			keyStruct{pattern, pattern, ast.TokenRegexEqual}:    errors.New("mismatched type to binary operator. got regex =~ regex. see bool(), int(), float(), string(), duration()"),
			keyStruct{pattern, pattern, ast.TokenRegexNotEqual}: errors.New("mismatched type to binary operator. got regex !~ regex. see bool(), int(), float(), string(), duration()"),
			keyStruct{pattern, pattern, ast.TokenEqual}:         errors.New("invalid comparison operator == for type regex"),
		})
}

func TestExpression_EvalBool_NotSupportedValueLeft(t *testing.T) {
	scope := stateful.NewScope()
	scope.Set("value", []int{1, 2, 3})
	_, err := evalCompiledBoolWithScope(t, scope, &ast.BinaryNode{
		Operator: ast.TokenEqual,
		Left: &ast.ReferenceNode{
			Reference: "value",
		},
		Right: &ast.StringNode{
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
	_, err = evalCompiledBoolWithScope(t, scope, &ast.BinaryNode{
		Operator: ast.TokenEqual,
		Left: &ast.StringNode{
			Literal: "yo",
		},
		Right: &ast.ReferenceNode{
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
	node := &ast.BinaryNode{
		Operator: ast.TokenType(666),
		Left: &ast.StringNode{
			Literal: "value",
		},
		Right: &ast.StringNode{
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
	emptyScope := stateful.NewScope()
	expectedError := `name "value" is undefined. Names in scope: `

	// Check left side
	_, err := evalCompiledBoolWithScope(t, emptyScope, &ast.BinaryNode{
		Operator: ast.TokenEqual,
		Left: &ast.ReferenceNode{
			Reference: "value",
		},
		Right: &ast.StringNode{
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
	_, err = evalCompiledBoolWithScope(t, emptyScope, &ast.BinaryNode{
		Operator: ast.TokenEqual,
		Left: &ast.StringNode{
			Literal: "yo",
		},
		Right: &ast.ReferenceNode{
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
	scope := stateful.NewScope()
	scope.Set("value", nil)

	expectedError := `referenced value "value" is nil.`

	// Check left side
	_, err := evalCompiledBoolWithScope(t, scope, &ast.BinaryNode{
		Operator: ast.TokenEqual,
		Left: &ast.ReferenceNode{
			Reference: "value",
		},
		Right: &ast.StringNode{
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
	_, err = evalCompiledBoolWithScope(t, scope, &ast.BinaryNode{
		Operator: ast.TokenEqual,
		Left: &ast.StringNode{
			Literal: "yo",
		},
		Right: &ast.ReferenceNode{
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
	scope := stateful.NewScope()

	// First Case - true as boolValue
	boolValue := true

	scope.Set("boolValue", boolValue)
	result, err := evalCompiledBoolWithScope(t, scope, &ast.ReferenceNode{
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
	result, err = evalCompiledBoolWithScope(t, scope, &ast.ReferenceNode{
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
	emptyScope := stateful.NewScope()
	expectedError := `name "value" is undefined. Names in scope: `

	// Check left side
	se := mustCompileExpression(&ast.ReferenceNode{
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
	expectedError := `TypeGuard: expression returned unexpected type invalid type, expected boolean`

	scope := stateful.NewScope()
	scope.Set("value", []int{1, 2, 3})

	// Check left side
	_, err := evalCompiledBoolWithScope(t, scope, &ast.ReferenceNode{
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
	emptyScope := stateful.NewScope()
	expectedError := `name "value" is undefined. Names in scope: `

	// Check left side
	_, err := evalCompiledBoolWithScope(t, emptyScope, &ast.BinaryNode{
		Operator: ast.TokenGreater,
		Left: &ast.ReferenceNode{
			Reference: "value",
		},
		Right: &ast.NumberNode{
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
	se, err := stateful.NewExpression(&ast.BinaryNode{
		Operator: ast.TokenPlus,
		Left: &ast.StringNode{
			Literal: "left",
		},
		Right: &ast.StringNode{
			Literal: "right",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	scope := stateful.NewScope()
	result, err := se.EvalString(scope)
	if err != nil {
		t.Fatal("unexpected error EvalString:", err)
	}
	if exp := "leftright"; exp != result {
		t.Errorf("unexpected EvalString results: got %s exp %s", result, exp)
	}
}

func TestExpression_EvalString_StringConcatReferenceNode(t *testing.T) {
	se, err := stateful.NewExpression(&ast.BinaryNode{
		Operator: ast.TokenPlus,
		Left: &ast.StringNode{
			Literal: "left",
		},
		Right: &ast.ReferenceNode{
			Reference: "value",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	scope := stateful.NewScope()
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
	se := mustCompileExpression(&ast.BinaryNode{
		Operator: ast.TokenLess,
		Left: &ast.UnaryNode{
			Operator: ast.TokenMinus,
			Node: &ast.ReferenceNode{
				Reference: "value",
			},
		},
		Right: &ast.NumberNode{
			IsInt: true,
			Int64: int64(0),
		},
	})

	scope := stateful.NewScope()
	scope.Set("value", int64(4))
	result, err := se.EvalBool(scope)
	if err != nil {
		t.Errorf("Ref node: Failed to evaluate:\n%v", err)
	}

	if !result {
		t.Errorf("int ref test case: unexpected result: got: %t, expected: true", result)
	}

}

func TestExpression_EvalBool_BinaryNodeWithBoolUnaryNode(t *testing.T) {

	emptyScope := stateful.NewScope()

	se := mustCompileExpression(&ast.BinaryNode{
		Operator: ast.TokenEqual,
		Left: &ast.UnaryNode{
			Operator: ast.TokenNot,
			Node: &ast.BoolNode{
				Bool: false,
			},
		},
		Right: &ast.BoolNode{
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
	se = mustCompileExpression(&ast.BinaryNode{
		Operator: ast.TokenEqual,
		Left: &ast.UnaryNode{
			Operator: ast.TokenNot,
			Node: &ast.ReferenceNode{
				Reference: "value",
			},
		},
		Right: &ast.BoolNode{
			Bool: true,
		},
	})

	scope := stateful.NewScope()
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

	scope := stateful.NewScope()

	se := mustCompileExpression(&ast.BinaryNode{
		Operator: ast.TokenLess,
		Left: &ast.UnaryNode{
			Operator: ast.TokenMinus,
			Node: &ast.NumberNode{
				IsInt: true,
				Int64: 4,
			},
		},
		Right: &ast.NumberNode{
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

	scope := stateful.NewScope()

	// passing
	scope.Set("a", int64(11))
	scope.Set("b", int64(5))

	// a > 10 and b < 10
	se := mustCompileExpression(&ast.BinaryNode{
		Operator: ast.TokenAnd,

		Left: &ast.BinaryNode{
			Operator: ast.TokenGreater,
			Left: &ast.ReferenceNode{
				Reference: "a",
			},
			Right: &ast.NumberNode{
				IsInt: true,
				Int64: 10,
			},
		},

		Right: &ast.BinaryNode{
			Operator: ast.TokenLess,
			Left: &ast.ReferenceNode{
				Reference: "b",
			},
			Right: &ast.NumberNode{
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

	scope := stateful.NewScope()

	// passing
	scope.Set("a", int64(11))
	scope.Set("b", int64(5))

	// a > 10 and b < 10
	se := mustCompileExpression(&ast.BinaryNode{
		Operator: ast.TokenAnd,

		Left: &ast.BinaryNode{
			Operator: ast.TokenGreater,
			Left: &ast.ReferenceNode{
				Reference: "a",
			},
			// right = 5 * 2 = 10
			Right: &ast.BinaryNode{
				Operator: ast.TokenMult,
				Left: &ast.NumberNode{
					IsInt: true,
					Int64: 5,
				},
				Right: &ast.NumberNode{
					IsInt: true,
					Int64: 2,
				},
			},
		},

		Right: &ast.BinaryNode{
			Operator: ast.TokenLess,
			Left: &ast.ReferenceNode{
				Reference: "b",
			},
			Right: &ast.NumberNode{
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

	scope := stateful.NewScope()

	// passing
	scope.Set("a", float64(11))
	scope.Set("b", float64(5))

	// a > 10 and b < 10
	se := mustCompileExpression(&ast.BinaryNode{
		Operator: ast.TokenAnd,

		Left: &ast.BinaryNode{
			Operator: ast.TokenGreater,
			Left: &ast.ReferenceNode{
				Reference: "a",
			},
			// right = 5 * 2 = 10
			Right: &ast.BinaryNode{
				Operator: ast.TokenMult,
				Left: &ast.NumberNode{
					IsFloat: true,
					Float64: 5,
				},
				Right: &ast.NumberNode{
					IsFloat: true,
					Float64: 2,
				},
			},
		},

		Right: &ast.BinaryNode{
			Operator: ast.TokenLess,
			Left: &ast.ReferenceNode{
				Reference: "b",
			},
			Right: &ast.NumberNode{
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

	operators := []ast.TokenType{
		ast.TokenPlus,
		ast.TokenMinus,
		ast.TokenMult,
		ast.TokenDiv,
		ast.TokenMod,
	}

	createNumberNode := func(v interface{}) ast.Node {
		switch value := v.(type) {
		case float64:
			return &ast.NumberNode{
				IsFloat: true,
				Float64: value,
			}
		case int64:
			return &ast.NumberNode{
				IsInt: true,
				Int64: value,
			}
		// For the error case
		case string:
			return &ast.StringNode{
				Literal: value,
			}
		default:
			t.Fatalf("value supplied to createNumberNode is not string/int/float64: %t", v)
			return nil
		}
	}

	runCompiledNumericTests(t, createNumberNode, leftValues, rightValues, operators, map[keyStruct]interface{}{
		// Left is float64(5), Right is float64(5)
		keyStruct{float64(5), float64(5), ast.TokenPlus}:  float64(10),
		keyStruct{float64(5), float64(5), ast.TokenMinus}: float64(0),
		keyStruct{float64(5), float64(5), ast.TokenMult}:  float64(25),
		keyStruct{float64(5), float64(5), ast.TokenDiv}:   float64(1),

		// Left is int64(5), Right is int64(5)
		keyStruct{int64(5), int64(5), ast.TokenPlus}:  int64(10),
		keyStruct{int64(5), int64(5), ast.TokenMinus}: int64(0),
		keyStruct{int64(5), int64(5), ast.TokenMult}:  int64(25),
		keyStruct{int64(5), int64(5), ast.TokenDiv}:   int64(1),
		keyStruct{int64(5), int64(5), ast.TokenMod}:   int64(0),

		// Left is float64(5), Right is float64(10)
		keyStruct{float64(5), float64(10), ast.TokenPlus}:  float64(15),
		keyStruct{float64(5), float64(10), ast.TokenMinus}: float64(-5),
		keyStruct{float64(5), float64(10), ast.TokenMult}:  float64(50),
		keyStruct{float64(5), float64(10), ast.TokenDiv}:   float64(0.5),

		// Left is float64(10), Right is float64(5)
		keyStruct{float64(10), float64(5), ast.TokenPlus}:  float64(15),
		keyStruct{float64(10), float64(5), ast.TokenMinus}: float64(5),
		keyStruct{float64(10), float64(5), ast.TokenMult}:  float64(50),
		keyStruct{float64(10), float64(5), ast.TokenDiv}:   float64(2),

		// Left is float64(10), Right is float64(10)
		keyStruct{float64(10), float64(10), ast.TokenPlus}:  float64(20),
		keyStruct{float64(10), float64(10), ast.TokenMinus}: float64(0),
		keyStruct{float64(10), float64(10), ast.TokenMult}:  float64(100),
		keyStruct{float64(10), float64(10), ast.TokenDiv}:   float64(1),
	}, map[keyStruct]error{
		// Modulo token where left is float
		keyStruct{float64(5), float64(5), ast.TokenMod}:       errors.New("invalid math operator % for type float"),
		keyStruct{float64(5), float64(10), ast.TokenMod}:      errors.New("invalid math operator % for type float"),
		keyStruct{float64(10), float64(5), ast.TokenMod}:      errors.New("invalid math operator % for type float"),
		keyStruct{float64(10), float64(10), ast.TokenMod}:     errors.New("invalid math operator % for type float"),
		keyStruct{float64(5), int64(5), ast.TokenMod}:         errors.New("invalid math operator % for type float"),
		keyStruct{float64(10), int64(5), ast.TokenMod}:        errors.New("invalid math operator % for type float"),
		keyStruct{float64(10), "NON_INT_VALUE", ast.TokenMod}: errors.New("invalid math operator % for type float"),
		keyStruct{float64(5), "NON_INT_VALUE", ast.TokenMod}:  errors.New("invalid math operator % for type float"),

		// Left is int, right is float
		keyStruct{int64(5), float64(5), ast.TokenPlus}:   errors.New("mismatched type to binary operator. got int + float. see bool(), int(), float(), string(), duration()"),
		keyStruct{int64(5), float64(5), ast.TokenMinus}:  errors.New("mismatched type to binary operator. got int - float. see bool(), int(), float(), string(), duration()"),
		keyStruct{int64(5), float64(5), ast.TokenMult}:   errors.New("mismatched type to binary operator. got int * float. see bool(), int(), float(), string(), duration()"),
		keyStruct{int64(5), float64(5), ast.TokenDiv}:    errors.New("mismatched type to binary operator. got int / float. see bool(), int(), float(), string(), duration()"),
		keyStruct{int64(5), float64(5), ast.TokenMod}:    errors.New("mismatched type to binary operator. got int % float. see bool(), int(), float(), string(), duration()"),
		keyStruct{int64(5), float64(10), ast.TokenPlus}:  errors.New("mismatched type to binary operator. got int + float. see bool(), int(), float(), string(), duration()"),
		keyStruct{int64(5), float64(10), ast.TokenMinus}: errors.New("mismatched type to binary operator. got int - float. see bool(), int(), float(), string(), duration()"),
		keyStruct{int64(5), float64(10), ast.TokenMult}:  errors.New("mismatched type to binary operator. got int * float. see bool(), int(), float(), string(), duration()"),
		keyStruct{int64(5), float64(10), ast.TokenDiv}:   errors.New("mismatched type to binary operator. got int / float. see bool(), int(), float(), string(), duration()"),
		keyStruct{int64(5), float64(10), ast.TokenMod}:   errors.New("mismatched type to binary operator. got int % float. see bool(), int(), float(), string(), duration()"),

		// Left is float, right is int
		keyStruct{float64(5), int64(5), ast.TokenPlus}:  errors.New("mismatched type to binary operator. got float + int. see bool(), int(), float(), string(), duration()"),
		keyStruct{float64(5), int64(5), ast.TokenMinus}: errors.New("mismatched type to binary operator. got float - int. see bool(), int(), float(), string(), duration()"),
		keyStruct{float64(5), int64(5), ast.TokenMult}:  errors.New("mismatched type to binary operator. got float * int. see bool(), int(), float(), string(), duration()"),
		keyStruct{float64(5), int64(5), ast.TokenDiv}:   errors.New("mismatched type to binary operator. got float / int. see bool(), int(), float(), string(), duration()"),

		keyStruct{float64(10), int64(5), ast.TokenPlus}:  errors.New("mismatched type to binary operator. got float + int. see bool(), int(), float(), string(), duration()"),
		keyStruct{float64(10), int64(5), ast.TokenMinus}: errors.New("mismatched type to binary operator. got float - int. see bool(), int(), float(), string(), duration()"),
		keyStruct{float64(10), int64(5), ast.TokenMult}:  errors.New("mismatched type to binary operator. got float * int. see bool(), int(), float(), string(), duration()"),
		keyStruct{float64(10), int64(5), ast.TokenDiv}:   errors.New("mismatched type to binary operator. got float / int. see bool(), int(), float(), string(), duration()"),

		// Left is int, Right is "NON_INT_VALUE"
		keyStruct{int64(5), "NON_INT_VALUE", ast.TokenPlus}:  errors.New("mismatched type to binary operator. got int + string. see bool(), int(), float(), string(), duration()"),
		keyStruct{int64(5), "NON_INT_VALUE", ast.TokenMinus}: errors.New("mismatched type to binary operator. got int - string. see bool(), int(), float(), string(), duration()"),
		keyStruct{int64(5), "NON_INT_VALUE", ast.TokenMult}:  errors.New("mismatched type to binary operator. got int * string. see bool(), int(), float(), string(), duration()"),
		keyStruct{int64(5), "NON_INT_VALUE", ast.TokenDiv}:   errors.New("mismatched type to binary operator. got int / string. see bool(), int(), float(), string(), duration()"),
		keyStruct{int64(5), "NON_INT_VALUE", ast.TokenMod}:   errors.New("mismatched type to binary operator. got int % string. see bool(), int(), float(), string(), duration()"),

		// Left is float, Right is "NON_INT_VALUE"
		keyStruct{float64(5), "NON_INT_VALUE", ast.TokenPlus}:   errors.New("mismatched type to binary operator. got float + string. see bool(), int(), float(), string(), duration()"),
		keyStruct{float64(5), "NON_INT_VALUE", ast.TokenMinus}:  errors.New("mismatched type to binary operator. got float - string. see bool(), int(), float(), string(), duration()"),
		keyStruct{float64(5), "NON_INT_VALUE", ast.TokenMult}:   errors.New("mismatched type to binary operator. got float * string. see bool(), int(), float(), string(), duration()"),
		keyStruct{float64(5), "NON_INT_VALUE", ast.TokenDiv}:    errors.New("mismatched type to binary operator. got float / string. see bool(), int(), float(), string(), duration()"),
		keyStruct{float64(10), "NON_INT_VALUE", ast.TokenPlus}:  errors.New("mismatched type to binary operator. got float + string. see bool(), int(), float(), string(), duration()"),
		keyStruct{float64(10), "NON_INT_VALUE", ast.TokenMinus}: errors.New("mismatched type to binary operator. got float - string. see bool(), int(), float(), string(), duration()"),
		keyStruct{float64(10), "NON_INT_VALUE", ast.TokenMult}:  errors.New("mismatched type to binary operator. got float * string. see bool(), int(), float(), string(), duration()"),
		keyStruct{float64(10), "NON_INT_VALUE", ast.TokenDiv}:   errors.New("mismatched type to binary operator. got float / string. see bool(), int(), float(), string(), duration()"),
	})
}

func runCompiledNumericTests(
	t *testing.T,
	createNodeFn func(v interface{}) ast.Node,
	leftValues []interface{},
	rightValues []interface{},
	operators []ast.TokenType,
	expected map[keyStruct]interface{},
	errorExpectations map[keyStruct]error) {

	runCompiledEvalTests(t, func(t *testing.T, scope *stateful.Scope, n ast.Node) (interface{}, error) {
		se, err := stateful.NewExpression(n)
		if err != nil {
			return nil, err
		}
		return se.Eval(scope)
	}, createNodeFn, leftValues, rightValues, operators, expected, errorExpectations)
}

func runCompiledEvalBoolTests(
	t *testing.T,
	createNodeFn func(v interface{}) ast.Node,
	leftValues []interface{},
	rightValues []interface{},
	operators []ast.TokenType,
	expected map[keyStruct]interface{},
	errorExpectations map[keyStruct]error) {

	runCompiledEvalTests(t, evalCompiledBoolWithScope, createNodeFn, leftValues, rightValues, operators, expected, errorExpectations)
}

func evalCompiledBoolWithScope(t *testing.T, scope *stateful.Scope, n ast.Node) (interface{}, error) {
	se, err := stateful.NewExpression(n)
	if err != nil {
		return nil, err
	}
	return se.EvalBool(scope)
}

func runCompiledEvalTests(
	t *testing.T,
	evalNodeFn func(t *testing.T, scope *stateful.Scope, n ast.Node) (interface{}, error),
	createNodeFn func(v interface{}) ast.Node,
	leftValues []interface{},
	rightValues []interface{},
	operators []ast.TokenType,
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
				if isExpectedResultOk && isErrorOk {
					t.Fatalf("Found both an expected result and an expected error for: lhs: %v, rhs: %v, op: %v", lhs, rhs, op)
				}

				// Test simple const values compares
				emptyScope := stateful.NewScope()
				result, err := evalNodeFn(t, emptyScope, &ast.BinaryNode{
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
				t.Log("testing reference:", lhs, op, rhs)

				// Test left is reference while the right is const
				scope := stateful.NewScope()
				scope.Set("value", lhs)
				result, err = evalNodeFn(t, scope, &ast.BinaryNode{
					Operator: op,
					Left: &ast.ReferenceNode{
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

func mustCompileExpression(node ast.Node) stateful.Expression {
	se, err := stateful.NewExpression(node)
	if err != nil {
		panic(fmt.Sprintf("Failed to compile expression: %v", err))
	}

	return se
}
