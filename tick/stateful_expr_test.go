package tick_test

import (
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/influxdata/kapacitor/tick"
)

type keyStruct struct {
	lhs interface{}
	rhs interface{}
	op  tick.TokenType
}

func TestStatefulExpression_EvalBool_BoolNode(t *testing.T) {
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

	runEvalBoolTests(t, createBoolNode, leftValues, rightValues, operators, map[keyStruct]bool{
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
		keyStruct{true, "NON_BOOL_VALUE", tick.TokenLess}:  errors.New("mismatched type to binary operator. got bool < string. see bool(), int(), float()"),
		keyStruct{true, "NON_BOOL_VALUE", tick.TokenLess}:  errors.New("mismatched type to binary operator. got bool < string. see bool(), int(), float()"),
		keyStruct{false, "NON_BOOL_VALUE", tick.TokenLess}: errors.New("mismatched type to binary operator. got bool < string. see bool(), int(), float()"),
		keyStruct{false, "NON_BOOL_VALUE", tick.TokenLess}: errors.New("mismatched type to binary operator. got bool < string. see bool(), int(), float()"),

		// Left: True, Right: "NON_BOOL_VALUE"
		keyStruct{true, "NON_BOOL_VALUE", tick.TokenEqual}:    errors.New("mismatched type to binary operator. got bool == string. see bool(), int(), float()"),
		keyStruct{true, "NON_BOOL_VALUE", tick.TokenNotEqual}: errors.New("mismatched type to binary operator. got bool != string. see bool(), int(), float()"),
		keyStruct{true, "NON_BOOL_VALUE", tick.TokenAnd}:      errors.New("mismatched type to binary operator. got bool AND string. see bool(), int(), float()"),
		keyStruct{true, "NON_BOOL_VALUE", tick.TokenOr}:       errors.New("mismatched type to binary operator. got bool OR string. see bool(), int(), float()"),

		// Left: False, Right: "NON_BOOL_VALUE"
		keyStruct{false, "NON_BOOL_VALUE", tick.TokenEqual}:    errors.New("mismatched type to binary operator. got bool == string. see bool(), int(), float()"),
		keyStruct{false, "NON_BOOL_VALUE", tick.TokenNotEqual}: errors.New("mismatched type to binary operator. got bool != string. see bool(), int(), float()"),
		keyStruct{false, "NON_BOOL_VALUE", tick.TokenAnd}:      errors.New("mismatched type to binary operator. got bool AND string. see bool(), int(), float()"),
		keyStruct{false, "NON_BOOL_VALUE", tick.TokenOr}:       errors.New("mismatched type to binary operator. got bool OR string. see bool(), int(), float()"),

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

func TestStatefulExpression_EvalBool_NumberNode(t *testing.T) {
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

	runEvalBoolTests(t, createNumberNode, leftValues, rightValues, operators, map[keyStruct]bool{
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
		keyStruct{float64(5), float64(5), tick.TokenOr}:   errors.New("invalid float comparison operator OR"),
		keyStruct{float64(5), float64(10), tick.TokenOr}:  errors.New("invalid float comparison operator OR"),
		keyStruct{float64(5), int64(5), tick.TokenOr}:     errors.New("invalid float comparison operator OR"),
		keyStruct{float64(10), float64(5), tick.TokenOr}:  errors.New("invalid float comparison operator OR"),
		keyStruct{float64(10), float64(10), tick.TokenOr}: errors.New("invalid float comparison operator OR"),
		keyStruct{float64(10), int64(5), tick.TokenOr}:    errors.New("invalid float comparison operator OR"),
		keyStruct{int64(5), float64(5), tick.TokenOr}:     errors.New("invalid float comparison operator OR"),
		keyStruct{int64(5), float64(10), tick.TokenOr}:    errors.New("invalid float comparison operator OR"),
		keyStruct{int64(5), int64(5), tick.TokenOr}:       errors.New("invalid float comparison operator OR"),

		// (Redundant case)
		keyStruct{float64(5), "NON_INT_VALUE", tick.TokenOr}:  errors.New("mismatched type to binary operator. got float64 OR string. see bool(), int(), float()"),
		keyStruct{float64(10), "NON_INT_VALUE", tick.TokenOr}: errors.New("mismatched type to binary operator. got float64 OR string. see bool(), int(), float()"),
		keyStruct{int64(5), "NON_INT_VALUE", tick.TokenOr}:    errors.New("mismatched type to binary operator. got int64 OR string. see bool(), int(), float()"),

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

func TestStatefulExpression_EvalBool_StringNode(t *testing.T) {
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

	runEvalBoolTests(t, createStringNode, leftValues, rightValues, operators, map[keyStruct]bool{
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

		keyStruct{"a", int64(123), tick.TokenOr}: errors.New("mismatched type to binary operator. got string OR int64. see bool(), int(), float()"),
		keyStruct{"b", int64(123), tick.TokenOr}: errors.New("mismatched type to binary operator. got string OR int64. see bool(), int(), float()"),

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

func TestStatefulExpression_EvalBool_RegexNode(t *testing.T) {
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

	runEvalBoolTests(t, createStringOrRegexNode, leftValues, rightValues, operators, map[keyStruct]bool{
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

func TestStatefulExpression_EvalBool_NotSupportedValueLeft(t *testing.T) {
	scope := tick.NewScope()
	scope.Set("value", []int{1, 2, 3})
	_, err := evalBoolWithScope(t, scope, &tick.BinaryNode{
		Operator: tick.TokenEqual,
		Left: &tick.ReferenceNode{
			Reference: "value",
		},
		Right: &tick.StringNode{
			Literal: "yo",
		},
	})

	expectedError := "mismatched type to binary operator. got []int == string. see bool(), int(), float()"

	if err != nil && (err.Error() != expectedError) {
		t.Errorf("Unexpected error result: \ngot: %v\nexpected: %v", err.Error(), expectedError)
	}

	if err == nil {
		t.Error("Unexpected error result: but didn't got any error")
	}
}

func TestStatefulExpression_EvalBool_UnknownOperator(t *testing.T) {
	_, err := evalBool(t, &tick.BinaryNode{
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

func TestStatefulExpression_evalBinary_ReferenceNodeDosentExist(t *testing.T) {
	emptyScope := tick.NewScope()
	expectedError := `name "value" is undefined. Names in scope: `

	// Check left side
	_, err := evalBoolWithScope(t, emptyScope, &tick.BinaryNode{
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
	_, err = evalBoolWithScope(t, emptyScope, &tick.BinaryNode{
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

func TestStatefulExpression_EvalBool_ReturnsReferenceNode(t *testing.T) {
	scope := tick.NewScope()

	// First Case - true as boolValue
	boolValue := true

	scope.Set("boolValue", boolValue)
	result, err := evalBoolWithScope(t, scope, &tick.ReferenceNode{
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
	result, err = evalBoolWithScope(t, scope, &tick.ReferenceNode{
		Reference: "boolValue",
	})

	if err != nil {
		t.Errorf("Unexpected error result: %v", err.Error())
	}

	if result != boolValue {
		t.Errorf("Unexpected result: \ngot: %v\nexp: %v", result, boolValue)
	}
}

func TestStatefulExpression_EvalBool_ReferenceNodeDosentExist(t *testing.T) {
	emptyScope := tick.NewScope()
	expectedError := `name "value" is undefined. Names in scope: `

	// Check left side
	_, err := evalBoolWithScope(t, emptyScope, &tick.ReferenceNode{
		Reference: "value",
	})

	if err != nil && (err.Error() != expectedError) {
		t.Errorf("Unexpected error result: \ngot: %v\nexpected: %v", err.Error(), expectedError)
	}

	if err == nil {
		t.Error("Unexpected error result: but didn't got any error")
	}
}

func TestStatefulExpression_EvalBool_UnexpectedTypeResult(t *testing.T) {
	expectedError := `expression returned unexpected type []int`

	scope := tick.NewScope()
	scope.Set("value", []int{1, 2, 3})

	// Check left side
	_, err := evalBoolWithScope(t, scope, &tick.ReferenceNode{
		Reference: "value",
	})

	if err != nil && (err.Error() != expectedError) {
		t.Errorf("Unexpected error result: \ngot: %v\nexpected: %v", err.Error(), expectedError)
	}

	if err == nil {
		t.Error("Unexpected error result: but didn't got any error")
	}
}

func runEvalBoolTests(
	t *testing.T,
	createNodeFn func(v interface{}) tick.Node,
	leftValues []interface{},
	rightValues []interface{},
	operators []tick.TokenType,
	expected map[keyStruct]bool,
	errorExpectations map[keyStruct]error) {
	for _, lhs := range leftValues {
		for _, rhs := range rightValues {
			for _, op := range operators {

				key := keyStruct{lhs, rhs, op}
				exp, isBoolOk := expected[key]
				errorExpected, isErrorOk := errorExpectations[key]
				if !isBoolOk && !isErrorOk {
					t.Fatalf("Couldn't find an expected result/error for: lhs: %t, rhs: %t, op: %v", lhs, rhs, op)
				}

				// Test simple const values compares
				result, err := evalBool(t, &tick.BinaryNode{
					Operator: op,
					Left:     createNodeFn(lhs),
					Right:    createNodeFn(rhs),
				})

				// Expect value can be error or bool
				if isErrorOk && errorExpected.Error() != err.Error() {
					t.Errorf("unexpected error result: %t %v %t\ngot: %v\nexp: %v", lhs, op, rhs, err, errorExpected)
				} else if isBoolOk && exp != result {
					t.Errorf("unexpected bool result: %t %v %t\ngot: %v\nexp: %v", lhs, op, rhs, result, exp)
				}

				// Test left is reference while the right is const
				scope := tick.NewScope()
				scope.Set("value", lhs)
				result, err = evalBoolWithScope(t, scope, &tick.BinaryNode{
					Operator: op,
					Left: &tick.ReferenceNode{
						Reference: "value",
					},
					Right: createNodeFn(rhs),
				})

				if isErrorOk && errorExpected.Error() != err.Error() {
					t.Errorf("unexpected error result: %t %v %t\ngot: %v\nexp: %v", lhs, op, rhs, err, errorExpected)
				} else if isBoolOk && exp != result {
					t.Errorf("unexpected bool result: %t %v %t\ngot: %v\nexp: %v", lhs, op, rhs, result, exp)
				}

			}

		}
	}
}

func evalBool(t *testing.T, n tick.Node) (bool, error) {
	return evalBoolWithScope(t, tick.NewScope(), n)
}

func evalBoolWithScope(t *testing.T, scope *tick.Scope, n tick.Node) (bool, error) {
	se := tick.NewStatefulExpr(n)
	return se.EvalBool(scope)
}
