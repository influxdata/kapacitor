package stateful

import (
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/influxdata/kapacitor/tick"
)

func Test_valueTypeOf(t *testing.T) {
	type expectation struct {
		value     interface{}
		valueType ValueType
	}

	expectations := []expectation{
		{value: float64(0), valueType: TFloat64},
		{value: int64(0), valueType: TInt64},
		{value: "Kapacitor Rulz", valueType: TString},
		{value: true, valueType: TBool},
		{value: regexp.MustCompile("\\d"), valueType: TRegex},
		{value: t, valueType: InvalidType},
	}

	for _, expect := range expectations {
		result := valueTypeOf(expect.value)

		if result != expect.valueType {
			t.Errorf("Got unexpected result for valueTypeOf(%T):\ngot: %s\nexpected: %s", expect.value, result, expect.valueType)
		}

	}
}

func Test_findNodeTypes_constantType(t *testing.T) {
	constantTypes := []ValueType{TBool, TInt64, TFloat64}

	for _, valueType := range constantTypes {
		result, err := findNodeTypes(valueType, []NodeEvaluator{}, tick.NewScope(), CreateExecutionState())
		if err != nil {
			t.Errorf("Got error while trying to findNodeTypes for value type %q: %v", valueType, err)
			continue
		}

		if result != valueType {
			t.Errorf("Got unexpected result:\ngot: %s\nexpected: %s\n", result, valueType)
		}
	}
}

func Test_findNodeTypes_TNumeric(t *testing.T) {
	type expectation struct {
		evaluators []NodeEvaluator
		valueType  ValueType
	}

	expectations := []expectation{
		{
			evaluators: []NodeEvaluator{&EvalFloatNode{}},
			valueType:  TFloat64,
		},

		{
			evaluators: []NodeEvaluator{&EvalIntNode{}},
			valueType:  TInt64,
		},

		{
			evaluators: []NodeEvaluator{
				&EvalFloatNode{},
				&EvalIntNode{},
			},
			valueType: TFloat64,
		},
	}

	for _, expect := range expectations {
		result, err := findNodeTypes(TNumeric, expect.evaluators, tick.NewScope(), CreateExecutionState())
		if err != nil {
			t.Errorf("Got error while trying to findNodeTypes for value type %q: %v", expect.valueType, err)
			continue
		}

		if result != expect.valueType {
			t.Errorf("Got unexpected result:\ngot: %s\nexpected: %s\n", result, expect.valueType)
		}
	}
}

func Test_findNodeTypes_TNumericWithEvalError(t *testing.T) {
	refNodeEvaluator, err := createNodeEvaluator(&tick.ReferenceNode{Reference: "value"})
	if err != nil {
		t.Fatalf("Failed to create reference node evaluator for test: %v", err)
	}

	result, err := findNodeTypes(TNumeric, []NodeEvaluator{refNodeEvaluator}, tick.NewScope(), CreateExecutionState())
	expectedError := errors.New("name \"value\" is undefined. Names in scope:")

	if err == nil {
		t.Errorf("Expected an error, but got nil error and result: %q", result)
		return
	}

	if strings.TrimSpace(err.Error()) != expectedError.Error() {
		t.Errorf("Got unexpected error:\ngot: %v\nexpected: %v\n", err, expectedError)
	}
}
