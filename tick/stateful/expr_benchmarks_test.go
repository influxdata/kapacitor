package stateful_test

import (
	"fmt"
	"testing"

	"github.com/influxdata/kapacitor/tick/ast"
	"github.com/influxdata/kapacitor/tick/stateful"
)

/*
	Benchmarks for evaluating stateful expression

	We will test by few basic scenario:
		*) One operator
		*) Two operators - with AND and OR relationship

	And will test with reference node and without to check the
	isolation performance overhead of reference node

    Test name format:
        Benchmark{Evaluation type: EvalBool or EvalNum}_{Type: OneOperator, TwoOperator}_{LeftNode}_{RightNode}
*/

func BenchmarkEvalBool_OneOperator_UnaryNode_BoolNode(b *testing.B) {

	emptyScope := stateful.NewScope()
	benchmarkEvalBool(b, emptyScope, &ast.UnaryNode{
		Operator: ast.TokenNot,
		Node: &ast.BoolNode{
			Bool: false,
		},
	})
}

func BenchmarkEvalBool_OneOperator_UnaryNode_ReferenceNode(b *testing.B) {

	scope := stateful.NewScope()
	scope.Set("value", bool(false))

	benchmarkEvalBool(b, scope, &ast.UnaryNode{
		Operator: ast.TokenNot,
		Node: &ast.ReferenceNode{
			Reference: "value",
		},
	})
}

func BenchmarkEvalBool_OneOperator_NumberFloat64_NumberFloat64(b *testing.B) {

	emptyScope := stateful.NewScope()
	benchmarkEvalBool(b, emptyScope, &ast.BinaryNode{
		Operator: ast.TokenGreater,
		Left: &ast.NumberNode{
			IsFloat: true,
			Float64: float64(20),
		},
		Right: &ast.NumberNode{
			IsFloat: true,
			Float64: float64(10),
		},
	})
}

func BenchmarkEvalBool_OneOperator_NumberFloat64_NumberInt64(b *testing.B) {

	emptyScope := stateful.NewScope()
	benchmarkEvalBool(b, emptyScope, &ast.BinaryNode{
		Operator: ast.TokenGreater,
		Left: &ast.NumberNode{
			IsFloat: true,
			Float64: float64(20),
		},
		Right: &ast.NumberNode{
			IsInt: true,
			Int64: int64(10),
		},
	})
}

func BenchmarkEvalBool_OneOperator_NumberInt64_NumberInt64(b *testing.B) {
	emptyScope := stateful.NewScope()
	benchmarkEvalBool(b, emptyScope, &ast.BinaryNode{
		Operator: ast.TokenGreater,
		Left: &ast.NumberNode{
			IsInt: true,
			Int64: int64(20),
		},
		Right: &ast.NumberNode{
			IsInt: true,
			Int64: int64(10),
		},
	})
}

func BenchmarkEvalBool_OneOperator_UnaryNode(b *testing.B) {

	scope := stateful.NewScope()
	scope.Set("value", bool(true))

	benchmarkEvalBool(b, scope, &ast.BinaryNode{
		Operator: ast.TokenEqual,
		Left: &ast.UnaryNode{
			Operator: ast.TokenNot,
			Node: &ast.BoolNode{
				Bool: false,
			},
		},
		Right: &ast.ReferenceNode{
			Reference: "value",
		},
	})
}

func BenchmarkEvalBool_OneOperator_ReferenceNodeFloat64_NumberFloat64(b *testing.B) {

	scope := stateful.NewScope()
	scope.Set("value", float64(20))

	benchmarkEvalBool(b, scope, &ast.BinaryNode{
		Operator: ast.TokenGreater,
		Left: &ast.ReferenceNode{
			Reference: "value",
		},
		Right: &ast.NumberNode{
			IsFloat: true,
			Float64: float64(10),
		},
	})
}

func BenchmarkEvalBool_OneOperator_ReferenceNodeFloat64_NumberInt64(b *testing.B) {

	scope := stateful.NewScope()
	scope.Set("value", float64(20))

	benchmarkEvalBool(b, scope, &ast.BinaryNode{
		Operator: ast.TokenGreater,
		Left: &ast.ReferenceNode{
			Reference: "value",
		},
		Right: &ast.NumberNode{
			IsInt: true,
			Int64: int64(10),
		},
	})
}

func BenchmarkEvalBool_OneOperator_ReferenceNodeFloat64_ReferenceNodeFloat64(b *testing.B) {

	scope := stateful.NewScope()
	scope.Set("l", float64(20))
	scope.Set("r", float64(10))
	benchmarkEvalBool(b, scope, &ast.BinaryNode{
		Operator: ast.TokenGreater,
		Left: &ast.ReferenceNode{
			Reference: "l",
		},
		Right: &ast.ReferenceNode{
			Reference: "r",
		},
	})
}

func BenchmarkEvalBool_OneOperatorWith11ScopeItem_ReferenceNodeFloat64_NumberFloat64(b *testing.B) {

	scope := stateful.NewScope()
	scope.Set("value", float64(20))
	for i := 0; i < 10; i++ {
		scope.Set(fmt.Sprintf("value_%v", i), float64(i))
	}

	benchmarkEvalBool(b, scope, &ast.BinaryNode{
		Operator: ast.TokenGreater,
		Left: &ast.ReferenceNode{
			Reference: "value",
		},
		Right: &ast.NumberNode{
			IsFloat: true,
			Float64: float64(10),
		},
	})
}

func BenchmarkEvalBool_OneOperatorValueChanges_ReferenceNodeFloat64_NumberFloat64(b *testing.B) {

	scope := stateful.NewScope()
	initialValue := float64(20)

	scope.Set("value", initialValue)

	b.ReportAllocs()
	b.ResetTimer()

	se, err := stateful.NewExpression(&ast.BinaryNode{
		Operator: ast.TokenGreater,
		Left: &ast.ReferenceNode{
			Reference: "value",
		},
		Right: &ast.NumberNode{
			IsFloat: true,
			Float64: float64(10),
		},
	})
	if err != nil {
		b.Fatalf("Failed to compile the expression: %v", err)
	}

	// We have maximum value because we want to limit the maximum number in
	// the reference node so we don't get too much big numbers and the benchmark suite will increase our iterations number (b.N)
	currentValue := initialValue
	maximumValue := float64(40)

	var result bool
	for i := 0; i < b.N; i++ {
		b.StopTimer()

		currentValue += float64(1)
		if currentValue > maximumValue {
			currentValue = initialValue
		}

		scope.Set("value", currentValue)

		b.StartTimer()

		result, err := se.EvalBool(scope)
		if err != nil || !result {
			v, _ := scope.Get("value")
			b.Errorf("Failed to evaluate: error=%v, result=%t, value=%v, init=%v, maximum=%v", err, result, v, initialValue, maximumValue)
		}
	}

	evalBoolResult = result
}

func BenchmarkEvalBool_OneOperator_ReferenceNodeInt64_ReferenceNodeInt64(b *testing.B) {

	scope := stateful.NewScope()
	scope.Set("l", int64(20))
	scope.Set("r", int64(10))

	benchmarkEvalBool(b, scope, &ast.BinaryNode{
		Operator: ast.TokenGreater,
		Left: &ast.ReferenceNode{
			Reference: "l",
		},
		Right: &ast.ReferenceNode{
			Reference: "r",
		},
	})
}

func BenchmarkEvalBool_OneOperatorWith11ScopeItem_ReferenceNodeInt64_NumberInt64(b *testing.B) {

	scope := stateful.NewScope()
	scope.Set("value", int64(20))
	for i := 0; i < 10; i++ {
		scope.Set(fmt.Sprintf("value_%v", i), int64(i))
	}

	benchmarkEvalBool(b, scope, &ast.BinaryNode{
		Operator: ast.TokenGreater,
		Left: &ast.ReferenceNode{
			Reference: "value",
		},
		Right: &ast.NumberNode{
			IsInt: true,
			Int64: int64(10),
		},
	})
}

func BenchmarkEvalBool_TwoLevelDeep(b *testing.B) {
	scope := stateful.NewScope()
	scope.Set("a", float64(11))
	scope.Set("b", float64(8))

	benchmarkEvalBool(b, scope, &ast.BinaryNode{
		Operator: ast.TokenAnd,

		Left: &ast.BinaryNode{
			Operator: ast.TokenGreater,
			Left: &ast.ReferenceNode{
				Reference: "a",
			},
			Right: &ast.NumberNode{
				IsFloat: true,
				Float64: 10,
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
}

func BenchmarkEvalBool_OneOperatorValueChanges_ReferenceNodeInt64_NumberInt64(b *testing.B) {

	scope := stateful.NewScope()
	initialValue := int64(20)

	scope.Set("value", initialValue)

	b.ReportAllocs()
	b.ResetTimer()

	se, err := stateful.NewExpression(&ast.BinaryNode{
		Operator: ast.TokenGreater,
		Left: &ast.ReferenceNode{
			Reference: "value",
		},
		Right: &ast.NumberNode{
			IsInt: true,
			Int64: int64(10),
		},
	})
	if err != nil {
		b.Fatalf("Failed to compile the expression: %v", err)
	}

	// We have maximum value because we want to limit the maximum number in
	// the reference node so we don't get too much big numbers and the benchmark suite will increase our iterations number (b.N)
	currentValue := initialValue
	maximumValue := int64(40)

	var result bool
	for i := 0; i < b.N; i++ {
		b.StopTimer()

		currentValue += int64(1)
		if currentValue > maximumValue {
			currentValue = initialValue

		}
		scope.Set("value", currentValue)

		b.StartTimer()

		result, err := se.EvalBool(scope)
		if err != nil || !result {
			v, _ := scope.Get("value")
			b.Errorf("Failed to evaluate: error=%v, result=%t, value=%v, init=%v, maximum=%v", err, result, v, initialValue, maximumValue)
		}
	}

	evalBoolResult = result
}

var evalBoolResult bool

func benchmarkEvalBool(b *testing.B, scope *stateful.Scope, node ast.Node) {
	b.ReportAllocs()
	b.ResetTimer()

	var err error
	se, err := stateful.NewExpression(node)
	if err != nil {
		b.Fatalf("Failed to compile the expression: %v", err)
	}

	for i := 0; i < b.N; i++ {
		evalBoolResult, err = se.EvalBool(scope)
		if err != nil {
			b.Fatal(err)
		}
	}
}
