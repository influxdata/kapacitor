package stateful_test

import (
	"fmt"
	"testing"

	"github.com/influxdata/kapacitor/tick"
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
        Benchmark_{Evaluation type: EvalBool or EvalNum}_{Type: OneOperator, TwoOperator}_{LeftNode}_{RightNode}
*/

func Benchmark_EvalBool_OneOperator_UnaryNode_BoolNode(b *testing.B) {

	emptyScope := tick.NewScope()
	benchmarkEvalBool(b, emptyScope, &tick.UnaryNode{
		Operator: tick.TokenNot,
		Node: &tick.BoolNode{
			Bool: false,
		},
	})
}

func Benchmark_EvalBool_OneOperator_UnaryNode_ReferenceNode(b *testing.B) {

	scope := tick.NewScope()
	scope.Set("value", bool(false))

	benchmarkEvalBool(b, scope, &tick.UnaryNode{
		Operator: tick.TokenNot,
		Node: &tick.ReferenceNode{
			Reference: "value",
		},
	})
}

func Benchmark_EvalBool_OneOperator_NumberFloat64_NumberFloat64(b *testing.B) {

	emptyScope := tick.NewScope()
	benchmarkEvalBool(b, emptyScope, &tick.BinaryNode{
		Operator: tick.TokenGreater,
		Left: &tick.NumberNode{
			IsFloat: true,
			Float64: float64(20),
		},
		Right: &tick.NumberNode{
			IsFloat: true,
			Float64: float64(10),
		},
	})
}

func Benchmark_EvalBool_OneOperator_NumberFloat64_NumberInt64(b *testing.B) {

	emptyScope := tick.NewScope()
	benchmarkEvalBool(b, emptyScope, &tick.BinaryNode{
		Operator: tick.TokenGreater,
		Left: &tick.NumberNode{
			IsFloat: true,
			Float64: float64(20),
		},
		Right: &tick.NumberNode{
			IsInt: true,
			Int64: int64(10),
		},
	})
}

func Benchmark_EvalBool_OneOperator_NumberInt64_NumberInt64(b *testing.B) {
	emptyScope := tick.NewScope()
	benchmarkEvalBool(b, emptyScope, &tick.BinaryNode{
		Operator: tick.TokenGreater,
		Left: &tick.NumberNode{
			IsInt: true,
			Int64: int64(20),
		},
		Right: &tick.NumberNode{
			IsInt: true,
			Int64: int64(10),
		},
	})
}

func Benchmark_EvalBool_OneOperator_UnaryNode(b *testing.B) {

	scope := tick.NewScope()
	scope.Set("value", bool(true))

	benchmarkEvalBool(b, scope, &tick.BinaryNode{
		Operator: tick.TokenEqual,
		Left: &tick.UnaryNode{
			Operator: tick.TokenNot,
			Node: &tick.BoolNode{
				Bool: false,
			},
		},
		Right: &tick.ReferenceNode{
			Reference: "value",
		},
	})
}

func Benchmark_EvalBool_OneOperator_ReferenceNodeFloat64_NumberFloat64(b *testing.B) {

	scope := tick.NewScope()
	scope.Set("value", float64(20))

	benchmarkEvalBool(b, scope, &tick.BinaryNode{
		Operator: tick.TokenGreater,
		Left: &tick.ReferenceNode{
			Reference: "value",
		},
		Right: &tick.NumberNode{
			IsFloat: true,
			Float64: float64(10),
		},
	})
}

func Benchmark_EvalBool_OneOperator_ReferenceNodeFloat64_NumberInt64(b *testing.B) {

	scope := tick.NewScope()
	scope.Set("value", float64(20))

	benchmarkEvalBool(b, scope, &tick.BinaryNode{
		Operator: tick.TokenGreater,
		Left: &tick.ReferenceNode{
			Reference: "value",
		},
		Right: &tick.NumberNode{
			IsInt: true,
			Int64: int64(10),
		},
	})
}

func Benchmark_EvalBool_OneOperator_ReferenceNodeFloat64_ReferenceNodeFloat64(b *testing.B) {

	scope := tick.NewScope()
	scope.Set("l", float64(20))
	scope.Set("r", float64(10))
	benchmarkEvalBool(b, scope, &tick.BinaryNode{
		Operator: tick.TokenGreater,
		Left: &tick.ReferenceNode{
			Reference: "l",
		},
		Right: &tick.ReferenceNode{
			Reference: "r",
		},
	})
}

func Benchmark_EvalBool_OneOperatorWith11ScopeItem_ReferenceNodeFloat64_NumberFloat64(b *testing.B) {

	scope := tick.NewScope()
	scope.Set("value", float64(20))
	for i := 0; i < 10; i++ {
		scope.Set(fmt.Sprintf("value_%v", i), float64(i))
	}

	benchmarkEvalBool(b, scope, &tick.BinaryNode{
		Operator: tick.TokenGreater,
		Left: &tick.ReferenceNode{
			Reference: "value",
		},
		Right: &tick.NumberNode{
			IsFloat: true,
			Float64: float64(10),
		},
	})
}

func Benchmark_EvalBool_OneOperatorValueChanges_ReferenceNodeFloat64_NumberFloat64(b *testing.B) {

	scope := tick.NewScope()
	initialValue := float64(20)

	scope.Set("value", initialValue)

	b.ReportAllocs()
	b.ResetTimer()

	se, err := stateful.NewExpression(&tick.BinaryNode{
		Operator: tick.TokenGreater,
		Left: &tick.ReferenceNode{
			Reference: "value",
		},
		Right: &tick.NumberNode{
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

func Benchmark_EvalBool_OneOperator_ReferenceNodeInt64_ReferenceNodeInt64(b *testing.B) {

	scope := tick.NewScope()
	scope.Set("l", int64(20))
	scope.Set("r", int64(10))

	benchmarkEvalBool(b, scope, &tick.BinaryNode{
		Operator: tick.TokenGreater,
		Left: &tick.ReferenceNode{
			Reference: "l",
		},
		Right: &tick.ReferenceNode{
			Reference: "r",
		},
	})
}

func Benchmark_EvalBool_OneOperatorWith11ScopeItem_ReferenceNodeInt64_NumberInt64(b *testing.B) {

	scope := tick.NewScope()
	scope.Set("value", int64(20))
	for i := 0; i < 10; i++ {
		scope.Set(fmt.Sprintf("value_%v", i), int64(i))
	}

	benchmarkEvalBool(b, scope, &tick.BinaryNode{
		Operator: tick.TokenGreater,
		Left: &tick.ReferenceNode{
			Reference: "value",
		},
		Right: &tick.NumberNode{
			IsInt: true,
			Int64: int64(10),
		},
	})
}

func Benchmark_EvalBool_TwoLevelDeep(b *testing.B) {
	scope := tick.NewScope()
	scope.Set("a", float64(11))
	scope.Set("b", float64(8))

	benchmarkEvalBool(b, scope, &tick.BinaryNode{
		Operator: tick.TokenAnd,

		Left: &tick.BinaryNode{
			Operator: tick.TokenGreater,
			Left: &tick.ReferenceNode{
				Reference: "a",
			},
			Right: &tick.NumberNode{
				IsFloat: true,
				Float64: 10,
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
}

func Benchmark_EvalBool_OneOperatorValueChanges_ReferenceNodeInt64_NumberInt64(b *testing.B) {

	scope := tick.NewScope()
	initialValue := int64(20)

	scope.Set("value", initialValue)

	b.ReportAllocs()
	b.ResetTimer()

	se, err := stateful.NewExpression(&tick.BinaryNode{
		Operator: tick.TokenGreater,
		Left: &tick.ReferenceNode{
			Reference: "value",
		},
		Right: &tick.NumberNode{
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

func benchmarkEvalBool(b *testing.B, scope *tick.Scope, node tick.Node) {
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
