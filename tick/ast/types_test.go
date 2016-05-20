package ast_test

import (
	"regexp"
	"testing"
	"time"

	"github.com/influxdata/kapacitor/tick/ast"
)

func Test_TypeOf(t *testing.T) {
	type expectation struct {
		value     interface{}
		valueType ast.ValueType
	}

	expectations := []expectation{
		{value: float64(0), valueType: ast.TFloat},
		{value: int64(0), valueType: ast.TInt},
		{value: "Kapacitor Rulz", valueType: ast.TString},
		{value: true, valueType: ast.TBool},
		{value: regexp.MustCompile("\\d"), valueType: ast.TRegex},
		{value: time.Duration(5), valueType: ast.TDuration},
		{value: time.Time{}, valueType: ast.TTime},
		{value: t, valueType: ast.InvalidType},
	}

	for _, expect := range expectations {
		result := ast.TypeOf(expect.value)

		if result != expect.valueType {
			t.Errorf("Got unexpected result for valueTypeOf(%T):\ngot: %s\nexpected: %s", expect.value, result, expect.valueType)
		}

	}
}
