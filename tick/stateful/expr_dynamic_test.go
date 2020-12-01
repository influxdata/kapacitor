package stateful_test

import (
	"errors"
	"testing"

	"github.com/influxdata/kapacitor/tick/ast"
	"github.com/influxdata/kapacitor/tick/stateful"
)

type valueExpectation struct {
	Value          interface{}
	ExpectedResult interface{}
	ExpectedError  error

	// Should we EvalBool or EvalNum
	IsEvalBool bool
	IsEvalNum  bool
}

type testCase struct {
	Title string

	// Node which the test is evaluated upon, must contain ReferenceNode with ref to "value"
	Node ast.Node

	Expectations []valueExpectation
}

// runDyanmicTestCase is when we want to change the "dyanmism" of
// a node - type change or value change
func runDynamicTestCase(t *testing.T, tc testCase) {
	t.Helper()
	se := mustCompileExpression(tc.Node)

	for i, expectation := range tc.Expectations {
		scope := stateful.NewScope()
		scope.Set("value", expectation.Value)

		var result interface{}
		var err error
		evaluationType := ""

		if expectation.IsEvalBool {
			evaluationType = "EvalBool"
			result, err = se.EvalBool(scope)
		}

		if expectation.IsEvalNum {
			evaluationType = "EvalNum"
			result, err = se.Eval(scope)
		}

		if err != nil || expectation.ExpectedError != nil {
			if err == nil {
				t.Errorf("%s, %s, Iteration: %v: Expected %v error, but got nil", tc.Title, evaluationType, (i + 1), expectation.ExpectedError)

			} else if expectation.ExpectedError == nil {
				t.Errorf("%s: %s: Iteration %v: Got unexpected error while expecting for result:\n %v\n", tc.Title, evaluationType, (i + 1), err)
				continue
			} else if err.Error() != expectation.ExpectedError.Error() {
				t.Errorf("%s: %s: Iteration %v: Unexpected error:\ngot: %v\nexpected: %v\n", tc.Title, evaluationType, (i + 1), err, expectation.ExpectedError)
			}
		}

		if result != expectation.ExpectedResult {
			t.Errorf("%s: %s: Iteration %v: Unexpected result:\ngot: %t\nexpected: %t\n", tc.Title, evaluationType, (i + 1), result, expectation.ExpectedResult)
		}
	}

}

func TestExpression_BinaryNode_DynamicTestCases(t *testing.T) {
	t.Run("BinaryNode - EvalBool supports numeric type changes", func(t *testing.T) {
		runDynamicTestCase(t, testCase{
			Title: t.Name(),

			Node: &ast.BinaryNode{
				Operator: ast.TokenGreater,
				Left: &ast.ReferenceNode{
					Reference: "value",
				},
				Right: &ast.NumberNode{
					IsFloat: true,
					Float64: float64(10),
				},
			},

			Expectations: []valueExpectation{
				{IsEvalBool: true, Value: float64(20), ExpectedError: nil, ExpectedResult: true},
				{IsEvalBool: true, Value: int64(5), ExpectedError: nil, ExpectedResult: false},
			},
		})

	})
	t.Run("BinaryNode - Eval errors on integer division by zero", func(t *testing.T) {
		runDynamicTestCase(t, testCase{
			Title: t.Name(),

			Node: &ast.BinaryNode{
				Operator: ast.TokenDiv,
				Left: &ast.NumberNode{
					IsInt: true,
					Int64: 10,
				},
				Right: &ast.NumberNode{
					IsInt: true,
					Int64: 0,
				},
			},

			Expectations: []valueExpectation{
				{ExpectedError: errors.New("runtime error: integer divide by zero"), IsEvalNum: true},
			},
		})
	})

	t.Run("BinaryNode - EvalNum supports numeric type changes", func(t *testing.T) {
		runDynamicTestCase(t, testCase{
			Title: t.Name(),

			Node: &ast.BinaryNode{
				Operator: ast.TokenPlus,
				Left: &ast.ReferenceNode{
					Reference: "value",
				},
				Right: &ast.NumberNode{
					IsFloat: true,
					Float64: float64(10),
				},
			},

			Expectations: []valueExpectation{
				{
					IsEvalNum:      true,
					Value:          float64(20),
					ExpectedError:  nil,
					ExpectedResult: float64(30),
				},
				{
					IsEvalNum:      true,
					Value:          int64(5),
					ExpectedError:  errors.New("mismatched type to binary operator. got int + float. see bool(), int(), float(), string(), duration()"),
					ExpectedResult: nil,
				},
			},
		})
	})

	t.Run("BinaryNode - EvalNum supports numeric type changes", func(t *testing.T) {
		runDynamicTestCase(t, testCase{
			Title: t.Name(),

			Node: &ast.BinaryNode{
				Operator: ast.TokenGreater,
				Left: &ast.ReferenceNode{
					Reference: "value",
				},
				Right: &ast.NumberNode{
					IsFloat: true,
					Float64: float64(10),
				},
			},

			Expectations: []valueExpectation{
				{
					IsEvalBool:     true,
					Value:          float64(20),
					ExpectedError:  nil,
					ExpectedResult: true,
				},
				{
					IsEvalBool:     true,
					Value:          int64(5),
					ExpectedError:  nil,
					ExpectedResult: false,
				},
			},
		})
	})

	t.Run("BinaryNode - Nested BinaryNodes can determine correct type", func(t *testing.T) {
		runDynamicTestCase(t, testCase{
			Title: t.Name(),
			Node: &ast.BinaryNode{
				Operator: ast.TokenAnd,
				Left: &ast.BinaryNode{
					Operator: ast.TokenLess,
					Left: &ast.ReferenceNode{
						Reference: "value",
					},
					Right: &ast.NumberNode{
						IsFloat: true,
						Float64: float64(10),
					},
				},
				Right: &ast.BinaryNode{
					Operator: ast.TokenGreater,
					Left: &ast.ReferenceNode{
						Reference: "value",
					},
					Right: &ast.NumberNode{
						IsFloat: true,
						Float64: float64(7),
					},
				},
			},

			Expectations: []valueExpectation{
				{
					IsEvalBool:     true,
					Value:          float64(8),
					ExpectedError:  nil,
					ExpectedResult: true,
				},
				{
					IsEvalBool:     true,
					Value:          int64(5),
					ExpectedError:  nil,
					ExpectedResult: false,
				},
				{
					IsEvalBool:     true,
					Value:          int64(11),
					ExpectedError:  nil,
					ExpectedResult: false,
				},
			},
		})

	})
}
func TestExpression_UnaryNode_DyanmicTestCases(t *testing.T) {
	t.Run("UnaryNode - EvalNum supports numeric type changes", func(t *testing.T) {
		runDynamicTestCase(t, testCase{
			Title: t.Name(),

			Node: &ast.UnaryNode{
				Operator: ast.TokenMinus,
				Node: &ast.ReferenceNode{
					Reference: "value",
				},
			},

			Expectations: []valueExpectation{
				{
					IsEvalNum:      true,
					Value:          float64(20),
					ExpectedError:  nil,
					ExpectedResult: float64(-20),
				},
				{
					IsEvalNum:      true,
					Value:          int64(20),
					ExpectedError:  nil,
					ExpectedResult: int64(-20),
				},
			},
		})
	})

	t.Run("UnaryNode - EvalBool supports boolean value changes", func(t *testing.T) {
		runDynamicTestCase(t, testCase{
			Title: t.Name(),

			Node: &ast.UnaryNode{
				Operator: ast.TokenNot,
				Node: &ast.ReferenceNode{
					Reference: "value",
				},
			},

			Expectations: []valueExpectation{
				{
					IsEvalBool:     true,
					Value:          true,
					ExpectedError:  nil,
					ExpectedResult: false,
				},
				{
					IsEvalBool:     true,
					Value:          false,
					ExpectedError:  nil,
					ExpectedResult: true,
				},
			},
		})
	})
}
