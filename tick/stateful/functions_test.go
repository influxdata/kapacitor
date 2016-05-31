package stateful

import (
	"errors"
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
