package expr

import (
	"errors"
	"math"
)

// A callable function from within the expression
type Func func(...float64) (float64, error)

// Lookup for functions
type Funcs map[string]Func

// Return set of built-in Funcs
func Functions() Funcs {
	funcs := make(Funcs)

	s := &sigma{}
	funcs["sigma"] = s.call

	return funcs
}

type sigma struct {
	mean     float64
	variance float64
	m2       float64
	n        float64
}

// Computes the number of standard devaitions a given value is from the running mean.
func (s *sigma) call(args ...float64) (float64, error) {
	if len(args) != 1 {
		return 0, errors.New("sigma expected exactly one argument")
	}
	x := args[0]
	s.n++
	delta := x - s.mean
	s.mean = s.mean + delta/s.n
	s.m2 = s.m2 + delta*(x-s.mean)
	s.variance = s.m2 / (s.n - 1)

	if s.n < 2 {
		return 0, nil
	}
	return math.Abs(x-s.mean) / math.Sqrt(s.variance), nil
}
