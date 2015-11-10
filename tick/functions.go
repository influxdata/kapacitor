package tick

import (
	"errors"
	"fmt"
	"math"
	"strconv"
)

var ErrNotFloat = errors.New("value is not a float")

// A callable function from within the expression
type Func interface {
	Reset()
	Call(...interface{}) (interface{}, error)
}

// Lookup for functions
type Funcs map[string]Func

// Return set of built-in Funcs
func NewFunctions() Funcs {
	funcs := make(Funcs)

	funcs["sigma"] = &sigma{}
	funcs["bool"] = &boolean{}
	funcs["int"] = &integer{}
	funcs["float"] = &float{}
	funcs["count"] = &count{}

	return funcs
}

type boolean struct {
}

func (*boolean) Reset() {
}

// Converts the value to a boolean
func (*boolean) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 1 {
		return 0, errors.New("bool expects exactly one argument")
	}
	switch a := args[0].(type) {
	case bool:
		v = a
	case string:
		v, err = strconv.ParseBool(a)
	default:
		err = fmt.Errorf("cannot convert %T to boolean", a)
	}
	return
}

type integer struct {
}

func (*integer) Reset() {
}

// Converts the value to a integer
func (*integer) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 1 {
		return 0, errors.New("integer expects exactly one argument")
	}
	switch a := args[0].(type) {
	case int64:
		v = a
	case float64:
		v = int64(a)
	case string:
		v, err = strconv.ParseInt(a, 10, 64)
	default:
		err = fmt.Errorf("cannot convert %T to integer", a)
	}
	return
}

type float struct {
}

func (*float) Reset() {
}

// Converts the value to a float
func (*float) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 1 {
		return 0, errors.New("float expects exactly one argument")
	}
	switch a := args[0].(type) {
	case int64:
		v = float64(a)
	case float64:
		v = a
	case string:
		v, err = strconv.ParseFloat(a, 64)
	default:
		err = fmt.Errorf("cannot convert %T to float", a)
	}
	return
}

type count struct {
	n int64
}

func (c *count) Reset() {
	c.n = 0
}

// Counts the number of values processed.
func (c *count) Call(args ...interface{}) (v interface{}, err error) {
	c.n++
	return c.n, nil
}

type sigma struct {
	mean     float64
	variance float64
	m2       float64
	n        float64
}

func (s *sigma) Reset() {
	s.mean = 0
	s.variance = 0
	s.m2 = 0
	s.n = 0
}

// Computes the number of standard devaitions a given value is from the running mean.
func (s *sigma) Call(args ...interface{}) (interface{}, error) {
	if len(args) != 1 {
		return 0, errors.New("sigma expects exactly one argument")
	}
	x, ok := args[0].(float64)
	if !ok {
		return nil, ErrNotFloat
	}
	s.n++
	delta := x - s.mean
	s.mean = s.mean + delta/s.n
	s.m2 = s.m2 + delta*(x-s.mean)
	s.variance = s.m2 / (s.n - 1)

	if s.n < 2 {
		return float64(0), nil
	}
	return math.Abs(x-s.mean) / math.Sqrt(s.variance), nil
}
