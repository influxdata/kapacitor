package stateful

import (
	"errors"
	"regexp"
	"testing"
	"time"
)

func Test_Bool(t *testing.T) {
	f := &boolean{}

	testCases := []struct {
		arg interface{}
		exp bool
		err error
	}{
		{
			arg: true,
			exp: true,
		},
		{
			arg: false,
			exp: false,
		},
		{
			arg: int64(1),
			exp: true,
		},
		{
			arg: int64(0),
			exp: false,
		},
		{
			arg: float64(1),
			exp: true,
		},
		{
			arg: float64(0),
			exp: false,
		},
		{
			arg: "true",
			exp: true,
		},
		{
			arg: "false",
			exp: false,
		},
		{
			arg: "asdf",
			err: errors.New(`strconv.ParseBool: parsing "asdf": invalid syntax`),
		},
		{
			arg: nil,
			err: errors.New("cannot convert <nil> to boolean"),
		},
	}
	for _, tc := range testCases {
		result, err := f.Call(tc.arg)
		if tc.err != nil {
			if err == nil {
				t.Errorf("expected error from bool(%v) got nil exp %s", tc.arg, tc.err)
			} else if got, exp := err.Error(), tc.err.Error(); got != exp {
				t.Errorf("unexpected error from bool(%v) got %s exp %s", tc.arg, got, exp)
			}
			continue
		} else if err != nil {
			t.Errorf("unexpected error from bool(%v) %s", tc.arg, err)
			continue
		}

		if got, ok := result.(bool); ok {
			if got != tc.exp {
				t.Errorf("unexpected result from bool(%v) got: %t exp %t", tc.arg, got, tc.exp)
			}
		} else {
			t.Errorf("expected bool result from bool(%v) got %T", tc.arg, result)
		}
	}

	expErr := "bool expects exactly one argument"
	if _, err := f.Call(); err == nil {
		t.Error("expected error from call to bool()")
	} else if got := err.Error(); got != expErr {
		t.Errorf("unexpected error from call to bool() got %s exp %s", got, expErr)
	}
	if _, err := f.Call(nil, nil); err == nil {
		t.Error("expected error from call to bool()")
	} else if got := err.Error(); got != expErr {
		t.Errorf("unexpected error from call to bool() got %s exp %s", got, expErr)
	}
}
func Test_Int(t *testing.T) {
	f := &integer{}

	testCases := []struct {
		arg interface{}
		exp int64
		err error
	}{
		{
			arg: true,
			exp: 1,
		},
		{
			arg: false,
			exp: 0,
		},
		{
			arg: int64(1),
			exp: 1,
		},
		{
			arg: int64(0),
			exp: 0,
		},
		{
			arg: float64(1),
			exp: 1,
		},
		{
			arg: float64(0),
			exp: 0,
		},
		{
			arg: float64(1.1),
			exp: 1,
		},
		{
			arg: float64(2.9999),
			exp: 2,
		},
		{
			arg: "1",
			exp: 1,
		},
		{
			arg: "2",
			exp: 2,
		},
		{
			arg: "2.5",
			err: errors.New(`strconv.ParseInt: parsing "2.5": invalid syntax`),
		},
		{
			arg: 15 * time.Nanosecond,
			exp: 15,
		},
		{
			arg: nil,
			err: errors.New("cannot convert <nil> to integer"),
		},
	}
	for _, tc := range testCases {
		result, err := f.Call(tc.arg)
		if tc.err != nil {
			if err == nil {
				t.Errorf("expected error from int(%v) got nil exp %s", tc.arg, tc.err)
			} else if got, exp := err.Error(), tc.err.Error(); got != exp {
				t.Errorf("unexpected error from int(%v) got %s exp %s", tc.arg, got, exp)
			}
			continue
		} else if err != nil {
			t.Errorf("unexpected error from int(%v) %s", tc.arg, err)
			continue
		}

		if got, ok := result.(int64); ok {
			if got != tc.exp {
				t.Errorf("unexpected result from int(%v) got: %d exp %d", tc.arg, got, tc.exp)
			}
		} else {
			t.Errorf("expected int result from int(%v) got %T", tc.arg, result)
		}
	}
	expErr := "int expects exactly one argument"
	if _, err := f.Call(); err == nil {
		t.Error("expected error from call to int()")
	} else if got := err.Error(); got != expErr {
		t.Errorf("unexpected error from call to int() got %s exp %s", got, expErr)
	}
	if _, err := f.Call(nil, nil); err == nil {
		t.Error("expected error from call to int()")
	} else if got := err.Error(); got != expErr {
		t.Errorf("unexpected error from call to int() got %s exp %s", got, expErr)
	}
}
func Test_Float(t *testing.T) {
	f := &float{}

	testCases := []struct {
		arg interface{}
		exp float64
		err error
	}{
		{
			arg: true,
			exp: 1,
		},
		{
			arg: false,
			exp: 0,
		},
		{
			arg: int64(1),
			exp: 1,
		},
		{
			arg: int64(0),
			exp: 0,
		},
		{
			arg: float64(1),
			exp: 1,
		},
		{
			arg: float64(0),
			exp: 0,
		},
		{
			arg: float64(1.1),
			exp: 1.1,
		},
		{
			arg: float64(2.9999),
			exp: 2.9999,
		},
		{
			arg: "1",
			exp: 1,
		},
		{
			arg: "2",
			exp: 2,
		},
		{
			arg: "2.5",
			exp: 2.5,
		},
		{
			arg: "asdf",
			err: errors.New(`strconv.ParseFloat: parsing "asdf": invalid syntax`),
		},
		{
			arg: 15 * time.Nanosecond,
			err: errors.New("cannot convert time.Duration to float"),
		},
		{
			arg: nil,
			err: errors.New("cannot convert <nil> to float"),
		},
	}
	for _, tc := range testCases {
		result, err := f.Call(tc.arg)
		if tc.err != nil {
			if err == nil {
				t.Errorf("expected error from float(%v) got nil exp %s", tc.arg, tc.err)
			} else if got, exp := err.Error(), tc.err.Error(); got != exp {
				t.Errorf("unexpected error from float(%v) got %s exp %s", tc.arg, got, exp)
			}
			continue
		} else if err != nil {
			t.Errorf("unexpected error from float(%v) %s", tc.arg, err)
			continue
		}

		if got, ok := result.(float64); ok {
			if got != tc.exp {
				t.Errorf("unexpected result from float(%v) got: %f exp %f", tc.arg, got, tc.exp)
			}
		} else {
			t.Errorf("expected float result from float(%v) got %T", tc.arg, result)
		}
	}

	expErr := "float expects exactly one argument"
	if _, err := f.Call(); err == nil {
		t.Error("expected error from call to float()")
	} else if got := err.Error(); got != expErr {
		t.Errorf("unexpected error from call to float() got %s exp %s", got, expErr)
	}
	if _, err := f.Call(nil, nil); err == nil {
		t.Error("expected error from call to float()")
	} else if got := err.Error(); got != expErr {
		t.Errorf("unexpected error from call to float() got %s exp %s", got, expErr)
	}
}
func Test_String(t *testing.T) {
	f := &str{}

	testCases := []struct {
		arg interface{}
		exp string
		err error
	}{
		{
			arg: true,
			exp: "true",
		},
		{
			arg: false,
			exp: "false",
		},
		{
			arg: int64(1),
			exp: "1",
		},
		{
			arg: int64(0),
			exp: "0",
		},
		{
			arg: float64(1),
			exp: "1",
		},
		{
			arg: float64(0),
			exp: "0",
		},
		{
			arg: float64(1.1),
			exp: "1.1",
		},
		{
			arg: float64(2.9999),
			exp: "2.9999",
		},
		{
			arg: "1",
			exp: "1",
		},
		{
			arg: "2",
			exp: "2",
		},
		{
			arg: "2.5",
			exp: "2.5",
		},
		{
			arg: "asdf",
			exp: "asdf",
		},
		{
			arg: 15 * time.Second,
			exp: "15s",
		},
		{
			arg: nil,
			err: errors.New("cannot convert <nil> to string"),
		},
	}
	for _, tc := range testCases {
		result, err := f.Call(tc.arg)
		if tc.err != nil {
			if err == nil {
				t.Errorf("expected error from string(%v) got nil exp %s", tc.arg, tc.err)
			} else if got, exp := err.Error(), tc.err.Error(); got != exp {
				t.Errorf("unexpected error from string(%v) got %s exp %s", tc.arg, got, exp)
			}
			continue
		} else if err != nil {
			t.Errorf("unexpected error from string(%v) %s", tc.arg, err)
			continue
		}

		if got, ok := result.(string); ok {
			if got != tc.exp {
				t.Errorf("unexpected result from string(%v) got: %s exp %s", tc.arg, got, tc.exp)
			}
		} else {
			t.Errorf("expected string result from string(%v) got %T", tc.arg, result)
		}
	}

	expErr := "string expects exactly one argument"
	if _, err := f.Call(); err == nil {
		t.Error("expected error from call to string()")
	} else if got := err.Error(); got != expErr {
		t.Errorf("unexpected error from call to string() got %s exp %s", got, expErr)
	}
	if _, err := f.Call(nil, nil); err == nil {
		t.Error("expected error from call to string()")
	} else if got := err.Error(); got != expErr {
		t.Errorf("unexpected error from call to string() got %s exp %s", got, expErr)
	}
}

func Test_Duration(t *testing.T) {
	f := &duration{}

	testCases := []struct {
		args []interface{}
		exp  time.Duration
		err  error
	}{
		{
			args: []interface{}{true},
			err:  errors.New("cannot convert bool to duration"),
		},
		{
			args: []interface{}{false},
			err:  errors.New("cannot convert bool to duration"),
		},
		{
			args: []interface{}{int64(1), time.Second},
			exp:  1 * time.Second,
		},
		{
			args: []interface{}{int64(0), time.Nanosecond},
			exp:  0,
		},
		{
			args: []interface{}{float64(1), time.Hour},
			exp:  1 * time.Hour,
		},
		{
			args: []interface{}{float64(2.9999), time.Millisecond},
			exp:  time.Duration(2.9999 * float64(time.Millisecond)),
		},
		{
			args: []interface{}{"1m"},
			exp:  1 * time.Minute,
		},
		{
			args: []interface{}{"2s"},
			exp:  2 * time.Second,
		},
		{
			args: []interface{}{"2.5h"},
			err:  errors.New(`invalid duration string "2.5h"`),
		},
		{
			args: []interface{}{15 * time.Nanosecond},
			exp:  15,
		},
		{
			args: []interface{}{nil, time.Second},
			err:  errors.New("cannot convert <nil> to duration"),
		},
		{
			args: []interface{}{int64(1)},
			err:  errors.New("duration expects unit argument for int and float values"),
		},
		{
			args: []interface{}{int64(1), 0},
			err:  errors.New("invalid duration unit type: int"),
		},
		{
			args: []interface{}{float64(1)},
			err:  errors.New("duration expects unit argument for int and float values"),
		},
		{
			args: []interface{}{float64(1), 0},
			err:  errors.New("invalid duration unit type: int"),
		},
		{
			args: []interface{}{},
			err:  errors.New("duration expects one or two arguments duration(value, unit) where unit is optional depending on the type of value."),
		},
		{
			args: []interface{}{nil, nil, nil},
			err:  errors.New("duration expects one or two arguments duration(value, unit) where unit is optional depending on the type of value."),
		},
	}
	for _, tc := range testCases {
		result, err := f.Call(tc.args...)
		if tc.err != nil {
			if err == nil {
				t.Errorf("expected error from duration(%v) got nil exp %s", tc.args, tc.err)
			} else if got, exp := err.Error(), tc.err.Error(); got != exp {
				t.Errorf("unexpected error from duration(%v) got %s exp %s", tc.args, got, exp)
			}
			continue
		} else if err != nil {
			t.Errorf("unexpected error from duration(%v) %s", tc.args, err)
			continue
		}

		if got, ok := result.(time.Duration); ok {
			if got != tc.exp {
				t.Errorf("unexpected result from duration(%v) got: %d exp %d", tc.args, got, tc.exp)
			}
		} else {
			t.Errorf("expected duration result from duration(%v) got %T", tc.args, result)
		}
	}
}

func Test_If(t *testing.T) {
	f := &ifFunc{}

	testCases := []struct {
		args []interface{}
		exp  interface{}
		err  error
	}{
		// Error cases
		{
			args: []interface{}{true},
			err:  errors.New("if expects exactly three arguments"),
		},
		{
			args: []interface{}{true, 6},
			err:  errors.New("if expects exactly three arguments"),
		},
		{
			args: []interface{}{12, 6, false},
			err:  errors.New("first argument to if must be a condition with type of bool - got int"),
		},

		// Simple if
		{
			args: []interface{}{true, 1, 2},
			exp:  1,
		},
		{
			args: []interface{}{false, 1, 2},
			exp:  2,
		},

		// multiple types
		{
			args: []interface{}{false, 1, "1"},
			err:  errors.New("Different return types are not supported - second argument is int and third argument is string"),
		},
	}

	for _, tc := range testCases {
		result, err := f.Call(tc.args...)
		if tc.err != nil {
			if err == nil {
				t.Errorf("expected error from if(%v)\ngot: nil\nexp: exp %s", tc.args, tc.err)
			} else if got, exp := err.Error(), tc.err.Error(); got != exp {
				t.Errorf("unexpected error from if(%v)\ngot: %s\nexp: %s", tc.args, got, exp)
			}
			continue
		} else if err != nil {
			t.Errorf("unexpected error from if(%v) %s", tc.args, err)
			continue
		}

		if result != tc.exp {
			t.Errorf("unexpected result from if(%v)\ngot: %+v\nexp: %+v", tc.args, result, tc.exp)
		}

	}
}

func Test_StatelessFuncs(t *testing.T) {

	testCases := []struct {
		name string
		args []interface{}
		exp  interface{}
		err  error
	}{
		{
			name: "strContains",
			args: []interface{}{"aabbc", "bc"},
			exp:  true,
		},
		{
			name: "strContains",
			args: []interface{}{"aabbc", "bx"},
			exp:  false,
		},
		{
			name: "strContains",
			args: []interface{}{""},
			err:  errors.New("strContains expects exactly two arguments"),
		},
		{
			name: "strContainsAny",
			args: []interface{}{"aabbc", "bx"},
			exp:  true,
		},
		{
			name: "strContainsAny",
			args: []interface{}{"aabbc", "xy"},
			exp:  false,
		},
		{
			name: "strContainsAny",
			args: []interface{}{""},
			err:  errors.New("strContainsAny expects exactly two arguments"),
		},
		{
			name: "strCount",
			args: []interface{}{"aababc", "a"},
			exp:  int64(3),
		},
		{
			name: "strCount",
			args: []interface{}{"aabbc", "x"},
			exp:  int64(0),
		},
		{
			name: "strCount",
			args: []interface{}{""},
			err:  errors.New("strCount expects exactly two arguments"),
		},
		{
			name: "strHasPrefix",
			args: []interface{}{"aababc", "aab"},
			exp:  true,
		},
		{
			name: "strHasPrefix",
			args: []interface{}{"aabbc", "aax"},
			exp:  false,
		},
		{
			name: "strHasPrefix",
			args: []interface{}{""},
			err:  errors.New("strHasPrefix expects exactly two arguments"),
		},
		{
			name: "strHasSuffix",
			args: []interface{}{"aababc", "bc"},
			exp:  true,
		},
		{
			name: "strHasSuffix",
			args: []interface{}{"aabbc", "xbc"},
			exp:  false,
		},
		{
			name: "strHasSuffix",
			args: []interface{}{""},
			err:  errors.New("strHasSuffix expects exactly two arguments"),
		},
		{
			name: "strIndex",
			args: []interface{}{"aababc", "ba"},
			exp:  int64(2),
		},
		{
			name: "strIndex",
			args: []interface{}{"aabbc", "xbc"},
			exp:  int64(-1),
		},
		{
			name: "strIndex",
			args: []interface{}{""},
			err:  errors.New("strIndex expects exactly two arguments"),
		},
		{
			name: "strIndexAny",
			args: []interface{}{"aababc", "bax"},
			exp:  int64(0),
		},
		{
			name: "strIndexAny",
			args: []interface{}{"aabbc", "xy"},
			exp:  int64(-1),
		},
		{
			name: "strIndexAny",
			args: []interface{}{""},
			err:  errors.New("strIndexAny expects exactly two arguments"),
		},
		{
			name: "strLastIndex",
			args: []interface{}{"aababa", "ba"},
			exp:  int64(4),
		},
		{
			name: "strLastIndex",
			args: []interface{}{"aabbc", "xbc"},
			exp:  int64(-1),
		},
		{
			name: "strLastIndex",
			args: []interface{}{""},
			err:  errors.New("strLastIndex expects exactly two arguments"),
		},
		{
			name: "strLastIndexAny",
			args: []interface{}{"aababc", "bax"},
			exp:  int64(4),
		},
		{
			name: "strLastIndexAny",
			args: []interface{}{"aabbc", "xy"},
			exp:  int64(-1),
		},
		{
			name: "strLastIndexAny",
			args: []interface{}{""},
			err:  errors.New("strLastIndexAny expects exactly two arguments"),
		},
		{
			name: "strLength",
			args: []interface{}{""},
			exp:  int64(0),
		},
		{
			name: "strLength",
			args: []interface{}{"abxyzc"},
			exp:  int64(6),
		},
		{
			name: "strLength",
			args: []interface{}{},
			err:  errors.New("strLength expects exactly one argument"),
		},
		{
			name: "strLength",
			args: []interface{}{1},
			err:  errors.New("cannot pass int as first arg to strLength, must be string"),
		},
		{
			name: "strReplace",
			args: []interface{}{"abxyzc", "xyz", "", int64(1)},
			exp:  "abc",
		},
		{
			name: "strReplace",
			args: []interface{}{"abxaza", "a", "z", int64(-1)},
			exp:  "zbxzzz",
		},
		{
			name: "strReplace",
			args: []interface{}{"", "", ""},
			err:  errors.New("strReplace expects exactly four arguments"),
		},
		{
			name: "strSubstring",
			args: []interface{}{"abcdefg", int64(1), int64(5)},
			exp:  "bcde",
		},
		{
			name: "strSubstring",
			args: []interface{}{""},
			err:  errors.New("strSubstring expects exactly three arguments"),
		},
		{
			name: "strSubstring",
			args: []interface{}{"abcdefg", int64(0), int64(10)},
			err:  errors.New("stop index too large for string in strSubstring: 10"),
		},
		{
			name: "strSubstring",
			args: []interface{}{"abcdefg", int64(-1), int64(3)},
			err:  errors.New("found negative index for strSubstring: -1"),
		},
		{
			name: "strSubstring",
			args: []interface{}{"abcdefg", int64(0), int64(-3)},
			err:  errors.New("found negative index for strSubstring: -3"),
		},
		{
			name: "strToLower",
			args: []interface{}{"ABC"},
			exp:  "abc",
		},
		{
			name: "strToLower",
			args: []interface{}{"abc"},
			exp:  "abc",
		},
		{
			name: "strToLower",
			args: []interface{}{"", "", ""},
			err:  errors.New("strToLower expects exactly one argument"),
		},
		{
			name: "strToUpper",
			args: []interface{}{"ABC"},
			exp:  "ABC",
		},
		{
			name: "strToUpper",
			args: []interface{}{"abc"},
			exp:  "ABC",
		},
		{
			name: "strToUpper",
			args: []interface{}{"", "", ""},
			err:  errors.New("strToUpper expects exactly one argument"),
		},
		{
			name: "strTrim",
			args: []interface{}{"abba", "a"},
			exp:  "bb",
		},
		{
			name: "strTrim",
			args: []interface{}{"aabbaa", "a"},
			exp:  "bb",
		},
		{
			name: "strTrim",
			args: []interface{}{""},
			err:  errors.New("strTrim expects exactly two arguments"),
		},
		{
			name: "strTrimLeft",
			args: []interface{}{"abba", "a"},
			exp:  "bba",
		},
		{
			name: "strTrimLeft",
			args: []interface{}{"aabbaa", "a"},
			exp:  "bbaa",
		},
		{
			name: "strTrimLeft",
			args: []interface{}{""},
			err:  errors.New("strTrimLeft expects exactly two arguments"),
		},
		{
			name: "strTrimPrefix",
			args: []interface{}{"abba", "a"},
			exp:  "bba",
		},
		{
			name: "strTrimPrefix",
			args: []interface{}{"aabbaa", "a"},
			exp:  "abbaa",
		},
		{
			name: "strTrimPrefix",
			args: []interface{}{""},
			err:  errors.New("strTrimPrefix expects exactly two arguments"),
		},
		{
			name: "strTrimRight",
			args: []interface{}{"abba", "a"},
			exp:  "abb",
		},
		{
			name: "strTrimRight",
			args: []interface{}{"aabbaa", "a"},
			exp:  "aabb",
		},
		{
			name: "strTrimRight",
			args: []interface{}{""},
			err:  errors.New("strTrimRight expects exactly two arguments"),
		},
		{
			name: "strTrimSuffix",
			args: []interface{}{"abba", "a"},
			exp:  "abb",
		},
		{
			name: "strTrimSuffix",
			args: []interface{}{"aabbaa", "a"},
			exp:  "aabba",
		},
		{
			name: "strTrimSuffix",
			args: []interface{}{""},
			err:  errors.New("strTrimSuffix expects exactly two arguments"),
		},
		{
			name: "regexReplace",
			args: []interface{}{regexp.MustCompile("a(x*)b"), "-ab-axxb-", "T"},
			exp:  "-T-T-",
		},
		{
			name: "regexReplace",
			args: []interface{}{regexp.MustCompile("a(x*)b"), "-ab-axxb-", "$1"},
			exp:  "--xx-",
		},
		{
			name: "regexReplace",
			args: []interface{}{regexp.MustCompile("a(x*)b"), "-ab-axxb-", "$1W"},
			exp:  "---",
		},
		{
			name: "regexReplace",
			args: []interface{}{regexp.MustCompile("a(x*)b"), "-ab-axxb-", "${1}W"},
			exp:  "-W-xxW-",
		},
		{
			name: "regexReplace",
			args: []interface{}{regexp.MustCompile("a(?P<middle>x*)b"), "-ab-axxb-", "${middle}W"},
			exp:  "-W-xxW-",
		},
		{
			name: "regexReplace",
			args: []interface{}{""},
			err:  errors.New("regexReplace expects exactly three arguments"),
		},
	}

	for _, tc := range testCases {
		f, ok := statelessFuncs[tc.name]
		if !ok {
			t.Fatalf("unknown function %s", tc.name)
		}
		result, err := f.Call(tc.args...)
		if tc.err != nil {
			if err == nil {
				t.Errorf("%s: expected error got: nil exp: %s", tc.name, tc.err)
			} else if got, exp := err.Error(), tc.err.Error(); got != exp {
				t.Errorf("%s: unexpected error\ngot:\n%s\nexp:\n%s", tc.name, got, exp)
			}
			continue
		} else if err != nil {
			t.Errorf("%s: unexpected error: %s", tc.name, err)
			continue
		}

		if result != tc.exp {
			t.Errorf("%s: unexpected result\ngot: %+v\nexp: %+v", tc.name, result, tc.exp)
		}

	}

}
