package tick

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"
)

var ErrNotFloat = errors.New("value is not a float")

// A callable function from within the expression
type Func interface {
	Reset()
	Call(...interface{}) (interface{}, error)
}

// Lookup for functions
type Funcs map[string]Func

var statelessFuncs Funcs

func init() {
	statelessFuncs = make(Funcs)
	// Conversion functions
	statelessFuncs["bool"] = &boolean{}
	statelessFuncs["int"] = &integer{}
	statelessFuncs["float"] = &float{}
	statelessFuncs["string"] = &str{}

	// Math functions
	statelessFuncs["abs"] = newMath1("abs", math.Abs)
	statelessFuncs["acos"] = newMath1("acos", math.Acos)
	statelessFuncs["acosh"] = newMath1("acosh", math.Acosh)
	statelessFuncs["asin"] = newMath1("asin", math.Asin)
	statelessFuncs["asinh"] = newMath1("asinh", math.Asinh)
	statelessFuncs["atan"] = newMath1("atan", math.Atan)
	statelessFuncs["atan2"] = newMath2("atan2", math.Atan2)
	statelessFuncs["atanh"] = newMath1("atanh", math.Atanh)
	statelessFuncs["cbrt"] = newMath1("cbrt", math.Cbrt)
	statelessFuncs["ceil"] = newMath1("ceil", math.Ceil)
	statelessFuncs["cos"] = newMath1("cos", math.Cos)
	statelessFuncs["cosh"] = newMath1("cosh", math.Cosh)
	statelessFuncs["erf"] = newMath1("erf", math.Erf)
	statelessFuncs["erfc"] = newMath1("erfc", math.Erfc)
	statelessFuncs["exp"] = newMath1("exp", math.Exp)
	statelessFuncs["exp2"] = newMath1("exp2", math.Exp2)
	statelessFuncs["expm1"] = newMath1("expm1", math.Expm1)
	statelessFuncs["floor"] = newMath1("floor", math.Floor)
	statelessFuncs["gamma"] = newMath1("gamma", math.Gamma)
	statelessFuncs["hypot"] = newMath2("hypot", math.Hypot)
	statelessFuncs["j0"] = newMath1("j0", math.J0)
	statelessFuncs["j1"] = newMath1("j1", math.J1)
	statelessFuncs["jn"] = newMathIF("jn", math.Jn)
	statelessFuncs["log"] = newMath1("log", math.Log)
	statelessFuncs["log10"] = newMath1("log10", math.Log10)
	statelessFuncs["log1p"] = newMath1("log1p", math.Log1p)
	statelessFuncs["log2"] = newMath1("log2", math.Log2)
	statelessFuncs["logb"] = newMath1("logb", math.Logb)
	statelessFuncs["max"] = newMath2("max", math.Max)
	statelessFuncs["min"] = newMath2("min", math.Min)
	statelessFuncs["mod"] = newMath2("mod", math.Mod)
	statelessFuncs["pow"] = newMath2("pow", math.Pow)
	statelessFuncs["pow10"] = newMathI("pow10", math.Pow10)
	statelessFuncs["sin"] = newMath1("sin", math.Sin)
	statelessFuncs["sinh"] = newMath1("sinh", math.Sinh)
	statelessFuncs["sqrt"] = newMath1("sqrt", math.Sqrt)
	statelessFuncs["tan"] = newMath1("tan", math.Tan)
	statelessFuncs["tanh"] = newMath1("tanh", math.Tanh)
	statelessFuncs["trunc"] = newMath1("trunc", math.Trunc)
	statelessFuncs["y0"] = newMath1("y0", math.Y0)
	statelessFuncs["y1"] = newMath1("y1", math.Y1)
	statelessFuncs["yn"] = newMathIF("yn", math.Yn)

	// Time functions
	statelessFuncs["minute"] = &minute{}
	statelessFuncs["hour"] = &hour{}
	statelessFuncs["weekday"] = &weekday{}
	statelessFuncs["day"] = &day{}
	statelessFuncs["month"] = &month{}
	statelessFuncs["year"] = &year{}
}

// Return set of built-in Funcs
func NewFunctions() Funcs {
	funcs := make(Funcs, len(statelessFuncs)+2)
	for n, f := range statelessFuncs {
		funcs[n] = f
	}

	// Statefull functions -- need new instance
	funcs["sigma"] = &sigma{}
	funcs["count"] = &count{}

	return funcs
}

type math1Func func(float64) float64
type math1 struct {
	name string
	f    math1Func
}

func newMath1(name string, f math1Func) *math1 {
	return &math1{
		name: name,
		f:    f,
	}
}

// Converts the value to a boolean
func (m *math1) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 1 {
		return 0, errors.New(m.name + " expects exactly one argument")
	}
	a0, ok := args[0].(float64)
	if !ok {
		err = fmt.Errorf("cannot pass %T to %s, must be float64", args[0], m.name)
		return
	}
	v = m.f(a0)
	return
}

func (m *math1) Reset() {}

type math2Func func(float64, float64) float64
type math2 struct {
	name string
	f    math2Func
}

func newMath2(name string, f math2Func) *math2 {
	return &math2{
		name: name,
		f:    f,
	}
}

// Converts the value to a boolean
func (m *math2) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 2 {
		return 0, errors.New(m.name + " expects exactly two arguments")
	}
	a0, ok := args[0].(float64)
	if !ok {
		err = fmt.Errorf("cannot pass %T to %s, must be float64", args[0], m.name)
		return
	}
	a1, ok := args[1].(float64)
	if !ok {
		err = fmt.Errorf("cannot pass %T to %s, must be float64", args[1], m.name)
		return
	}
	v = m.f(a0, a1)
	return
}

func (m *math2) Reset() {}

type mathIFunc func(int) float64
type mathI struct {
	name string
	f    mathIFunc
}

func newMathI(name string, f mathIFunc) *mathI {
	return &mathI{
		name: name,
		f:    f,
	}
}

// Converts the value to a boolean
func (m *mathI) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 1 {
		return 0, errors.New(m.name + " expects exactly two arguments")
	}
	a0, ok := args[0].(int64)
	if !ok {
		err = fmt.Errorf("cannot pass %T to %s, must be int64", args[0], m.name)
		return
	}
	v = m.f(int(a0))
	return
}

func (m *mathI) Reset() {}

type mathIFFunc func(int, float64) float64
type mathIF struct {
	name string
	f    mathIFFunc
}

func newMathIF(name string, f mathIFFunc) *mathIF {
	return &mathIF{
		name: name,
		f:    f,
	}
}

// Converts the value to a boolean
func (m *mathIF) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 2 {
		return 0, errors.New(m.name + " expects exactly two arguments")
	}
	a0, ok := args[0].(int64)
	if !ok {
		err = fmt.Errorf("cannot pass %T to %s as first arg, must be int64", args[0], m.name)
		return
	}
	a1, ok := args[1].(float64)
	if !ok {
		err = fmt.Errorf("cannot pass %T to %s as second arg, must be float64", args[1], m.name)
		return
	}
	v = m.f(int(a0), a1)
	return
}

func (m *mathIF) Reset() {}

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
	case int64:
		v = a == 1
	case float64:
		v = a == 1.0
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
	case bool:
		if a {
			v = int64(1)
		} else {
			v = int64(0)
		}
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
	case bool:
		if a {
			v = float64(1)
		} else {
			v = float64(0)
		}
	default:
		err = fmt.Errorf("cannot convert %T to float", a)
	}
	return
}

type str struct {
}

func (*str) Reset() {
}

// Converts the value to a str
func (*str) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 1 {
		return 0, errors.New("str expects exactly one argument")
	}
	switch a := args[0].(type) {
	case int64:
		v = strconv.FormatInt(a, 64)
	case float64:
		v = strconv.FormatFloat(a, 'f', -1, 64)
	case bool:
		v = strconv.FormatBool(a)
	case string:
		v = a
	default:
		err = fmt.Errorf("cannot convert %T to string", a)
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

type minute struct {
}

func (*minute) Reset() {
}

// Return the minute within the hour for the given time, within the range [0,59].
func (*minute) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 1 {
		return 0, errors.New("minute expects exactly one argument")
	}
	switch a := args[0].(type) {
	case time.Time:
		v = int64(a.Minute())
	default:
		err = fmt.Errorf("cannot convert %T to time.Time", a)
	}
	return
}

type hour struct {
}

func (*hour) Reset() {
}

// Return the hour within the day for the given time, within the range [0,23].
func (*hour) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 1 {
		return 0, errors.New("hour expects exactly one argument")
	}
	switch a := args[0].(type) {
	case time.Time:
		v = int64(a.Hour())
	default:
		err = fmt.Errorf("cannot convert %T to time.Time", a)
	}
	return
}

type weekday struct {
}

func (*weekday) Reset() {
}

// Return the weekday within the week for the given time, within the range [0,6] where 0 is Sunday.
func (*weekday) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 1 {
		return 0, errors.New("weekday expects exactly one argument")
	}
	switch a := args[0].(type) {
	case time.Time:
		v = int64(a.Weekday())
	default:
		err = fmt.Errorf("cannot convert %T to time.Time", a)
	}
	return
}

type day struct {
}

func (*day) Reset() {
}

// Return the day within the month for the given time, within the range [1,31] depending on the month.
func (*day) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 1 {
		return 0, errors.New("day expects exactly one argument")
	}
	switch a := args[0].(type) {
	case time.Time:
		v = int64(a.Day())
	default:
		err = fmt.Errorf("cannot convert %T to time.Time", a)
	}
	return
}

type month struct {
}

func (*month) Reset() {
}

// Return the month within the year for the given time, within the range [1,12].
func (*month) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 1 {
		return 0, errors.New("month expects exactly one argument")
	}
	switch a := args[0].(type) {
	case time.Time:
		v = int64(a.Month())
	default:
		err = fmt.Errorf("cannot convert %T to time.Time", a)
	}
	return
}

type year struct {
}

func (*year) Reset() {
}

// Return the year for the given time.
func (*year) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 1 {
		return 0, errors.New("year expects exactly one argument")
	}
	switch a := args[0].(type) {
	case time.Time:
		v = int64(a.Year())
	default:
		err = fmt.Errorf("cannot convert %T to time.Time", a)
	}
	return
}
