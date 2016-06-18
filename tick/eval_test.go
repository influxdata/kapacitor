package tick_test

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/kapacitor/tick"
	"github.com/influxdata/kapacitor/tick/ast"
	"github.com/influxdata/kapacitor/tick/stateful"
)

//Test structure for evaluating a DSL

type structA struct {
	s *structB
}

type structB struct {
	Field1 string
	Field2 int64
	Field3 time.Duration
	c      *structC
}

type structC struct {
	field1  string `tick:"Options"`
	field2  float64
	field3  time.Duration
	AggFunc aggFunc
}

type orphan struct {
	parent *structA
	Sad    bool
	args   []interface{}
}

func (s *structA) StructB() *structB {
	return &structB{}
}

func (s *structB) StructC() *structC {
	s.c = &structC{}
	return s.c
}

func (s *structC) Options(str string, f float64, d time.Duration) *structC {
	s.field1 = str
	s.field2 = f
	s.field3 = d
	return s
}

type aggFunc func(values []float64) []float64

type influxql struct {
	Agg *agg
}

type agg struct {
	Sum aggFunc
}

func aggSum(values []float64) []float64 {
	s := 0.0
	for _, v := range values {
		s += v
	}
	return []float64{s}
}

func TestEvaluate(t *testing.T) {
	//Run a test that evaluates the DSL against the above structures.
	script := `
var s2 = a|structB()
			.field1('f1')
			.field2(42)

s2.field3(15m)

s2|structC()
	.options('c', 21.5, 7h)
	.aggFunc(influxql.agg.sum)
`

	scope := stateful.NewScope()
	a := &structA{}
	scope.Set("a", a)

	i := &influxql{
		Agg: &agg{
			Sum: aggSum,
		},
	}
	scope.Set("influxql", i)

	_, err := tick.Evaluate(script, scope, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	s2I, err := scope.Get("s2")
	if err != nil {
		t.Fatal(err)
	}
	s2 := s2I.(*structB)
	exp := structB{
		Field1: "f1",
		Field2: 42,
		Field3: time.Minute * 15,
	}

	s3 := *s2.c
	s2.c = nil
	if !reflect.DeepEqual(*s2, exp) {
		t.Errorf("unexpected s2 exp:%v got%v", exp, *s2)
	}
	c := structC{
		field1: "c",
		field2: 21.5,
		field3: time.Hour * 7,
	}
	aggFunc := s3.AggFunc
	s3.AggFunc = nil
	if !reflect.DeepEqual(s3, c) {
		t.Errorf("unexpected s3 exp:%v got%v", c, s3)
	}
	if exp, got := []float64{10.0}, aggFunc([]float64{5, 5}); !reflect.DeepEqual(exp, got) {
		t.Errorf("unexpected s3.AggFunc exp:%v got%v", exp, got)
	}
}

func TestEvaluate_DynamicMethod(t *testing.T) {
	script := `var x = a@dynamicMethod(1,'str', 10s).sad(FALSE)`

	scope := stateful.NewScope()
	a := &structA{}
	scope.Set("a", a)

	dm := func(self interface{}, args ...interface{}) (interface{}, error) {
		a, ok := self.(*structA)
		if !ok {
			return nil, fmt.Errorf("cannot call dynamicMethod on %T", self)
		}
		o := &orphan{
			parent: a,
			Sad:    true,
			args:   args,
		}
		return o, nil
	}
	scope.SetDynamicMethod("dynamicMethod", dm)

	_, err := tick.Evaluate(script, scope, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	xI, err := scope.Get("x")
	if err != nil {
		t.Fatal(err)
	}
	x, ok := xI.(*orphan)
	if !ok {
		t.Fatalf("expected x to be an *orphan, got %T", xI)
	}
	if x.Sad {
		t.Errorf("expected x to not be sad")
	}

	if got, exp := len(x.args), 3; exp != got {
		t.Fatalf("unexpected number of args: got %d exp %d", got, exp)
	}
	if got, exp := x.args[0], int64(1); exp != got {
		t.Errorf("unexpected x.args[0]: got %v exp %d", got, exp)
	}
	if got, exp := x.args[1], "str"; exp != got {
		t.Errorf("unexpected x.args[1]: got %v exp %s", got, exp)
	}
	if got, exp := x.args[2], time.Second*10; exp != got {
		t.Errorf("unexpected x.args[1]: got %v exp %v", got, exp)
	}
}

func TestValidateTemplate_Vars(t *testing.T) {
	script := `
var x = 3m
var y = -x

var n = TRUE
var m = !n

var z = x + y

var a string
`

	scope := stateful.NewScope()
	vars, err := tick.Evaluate(script, scope, nil, true)
	if err != nil {
		t.Fatal(err)
	}

	x, err := scope.Get("x")
	if err != nil {
		t.Fatal(err)
	}
	if value, ok := x.(time.Duration); ok {
		if exp, got := time.Minute*3, value; exp != got {
			t.Errorf("unexpected x value: exp %v got %v", exp, got)
		}
	} else {
		t.Errorf("unexpected x value type: exp time.Duration got %T", x)
	}

	y, err := scope.Get("y")
	if err != nil {
		t.Fatal(err)
	}
	if value, ok := y.(time.Duration); ok {
		if exp, got := time.Minute*-3, value; exp != got {
			t.Errorf("unexpected y value: exp %v got %v", exp, got)
		}
	} else {
		t.Errorf("unexpected y value type: exp time.Duration got %T", y)
	}

	n, err := scope.Get("n")
	if err != nil {
		t.Fatal(err)
	}
	if value, ok := n.(bool); ok {
		if exp, got := true, value; exp != got {
			t.Errorf("unexpected n value: exp %v got %v", exp, got)
		}
	} else {
		t.Errorf("unexpected m value type: exp bool got %T", x)
	}

	m, err := scope.Get("m")
	if err != nil {
		t.Fatal(err)
	}
	if value, ok := m.(bool); ok {
		if exp, got := false, value; exp != got {
			t.Errorf("unexpected m value: exp %v got %v", exp, got)
		}
	} else {
		t.Errorf("unexpected m value type: exp bool got %T", x)
	}

	z, err := scope.Get("z")
	if err != nil {
		t.Fatal(err)
	}
	if value, ok := z.(time.Duration); ok {
		if exp, got := time.Duration(0), value; exp != got {
			t.Errorf("unexpected z value: exp %v got %v", exp, got)
		}
	} else {
		t.Errorf("unexpected z value type: exp time.Duration got %T", z)
	}

	expVars := map[string]tick.Var{
		"a": {
			Value: nil,
			Type:  ast.TString,
		},
		"x": {
			Value: 3 * time.Minute,
			Type:  ast.TDuration,
		},
		"y": {
			Value: -3 * time.Minute,
			Type:  ast.TDuration,
		},
		"z": {
			Value: time.Duration(0),
			Type:  ast.TDuration,
		},
		"n": {
			Value: true,
			Type:  ast.TBool,
		},
		"m": {
			Value: false,
			Type:  ast.TBool,
		},
	}
	if !reflect.DeepEqual(expVars, vars) {
		t.Errorf("unexpected vars: got %v exp %v", vars, expVars)
	}
}
func TestEvaluate_Vars_PreDefined(t *testing.T) {
	script := `
var str = 'this is a string'
var strZero string

var strList = [ 'string list', str, *]
var strListZero list

var star = *
var starZero star

var int = 8
var intZero int

var float = 3.14
var floatZero float

var duration = 5m
var durationZero duration

var regex = /.*/
var regexZero regex

var boolean = TRUE
var booleanZero bool

var lambda = lambda: "value" > 0
var lambdaZero lambda
`
	lambda := &ast.LambdaNode{
		Expression: &ast.BinaryNode{
			Operator: ast.TokenGreater,
			Left: &ast.ReferenceNode{
				Reference: "value",
			},
			Right: &ast.NumberNode{
				IsInt: true,
				Int64: 0,
			},
		},
	}
	definedVars := map[string]tick.Var{
		"str": {
			Value: "asdf",
			Type:  ast.TString,
		},
		"strZero": {
			Value: "qwerty",
			Type:  ast.TString,
		},
		"strList": {
			Value: []tick.Var{
				{
					Value: "a",
					Type:  ast.TString,
				},
				{
					Value: "b",
					Type:  ast.TString,
				},
				{
					Value: "c",
					Type:  ast.TString,
				},
			},
			Type: ast.TList,
		},
		"strListZero": {
			Value: []tick.Var{
				{
					Value: &ast.StarNode{},
					Type:  ast.TStar,
				},
			},
			Type: ast.TList,
		},
		"star": {
			Value: &ast.StarNode{},
			Type:  ast.TStar,
		},
		"starZero": {
			Value: &ast.StarNode{},
			Type:  ast.TStar,
		},
		"int": {
			Value: int64(42),
			Type:  ast.TInt,
		},
		"intZero": {
			Value: int64(6 * 9),
			Type:  ast.TInt,
		},
		"float": {
			Value: float64(42),
			Type:  ast.TFloat,
		},
		"floatZero": {
			Value: float64(6 * 9),
			Type:  ast.TFloat,
		},
		"duration": {
			Value: 5 * time.Hour,
			Type:  ast.TDuration,
		},
		"durationZero": {
			Value: time.Minute,
			Type:  ast.TDuration,
		},
		"regex": {
			Value: regexp.MustCompile(`^asdf.*qwerty$`),
			Type:  ast.TRegex,
		},
		"regexZero": {
			Value: regexp.MustCompile(`^.*$`),
			Type:  ast.TRegex,
		},
		"boolean": {
			Value: false,
			Type:  ast.TBool,
		},
		"booleanZero": {
			Value: true,
			Type:  ast.TBool,
		},
		"lambda": {
			Value: lambda,
			Type:  ast.TLambda,
		},
		"lambdaZero": {
			Value: lambda,
			Type:  ast.TLambda,
		},
	}

	scope := stateful.NewScope()
	_, err := tick.Evaluate(script, scope, definedVars, false)
	if err != nil {
		t.Fatal(err)
	}

	expScope := map[string]interface{}{
		"str":          "asdf",
		"strZero":      "qwerty",
		"strList":      []interface{}{"a", "b", "c"},
		"strListZero":  []interface{}{&ast.StarNode{}},
		"star":         &ast.StarNode{},
		"starZero":     &ast.StarNode{},
		"int":          int64(42),
		"intZero":      int64(6 * 9),
		"float":        float64(42),
		"floatZero":    float64(6 * 9),
		"duration":     5 * time.Hour,
		"durationZero": time.Minute,
		"regex":        regexp.MustCompile(`^asdf.*qwerty$`),
		"regexZero":    regexp.MustCompile(`^.*$`),
		"boolean":      false,
		"booleanZero":  true,
		"lambda":       lambda,
		"lambdaZero":   lambda,
	}

	for name, value := range expScope {
		if got, err := scope.Get(name); err != nil {
			t.Errorf("unexpected error for %s: %s", name, err)
		} else if !equal(got, value) {
			t.Errorf("unexpected %s value: \ngot\n%v\ngot type: %T\nexp\n%v\nexp type: %T", name, got, got, value, value)
		}
	}

}

func TestEvaluate_Vars_ErrorWrongType(t *testing.T) {
	script := `
var x = 3m
`
	definedVars := map[string]tick.Var{
		"x": {
			Value: "5m",
			Type:  ast.TString,
		},
	}

	scope := stateful.NewScope()
	_, err := tick.Evaluate(script, scope, definedVars, false)
	if err == nil {
		t.Fatal("expected error for invalid var type")
	}
}
func TestEvaluate_Vars_ErrorMissingValue(t *testing.T) {
	script := `
var x duration
`
	scope := stateful.NewScope()
	if _, err := tick.Evaluate(script, scope, nil, false); err == nil {
		t.Fatal("expected error for missing var type")
	}

	if _, err := tick.Evaluate(script, scope, nil, true); err != nil {
		t.Fatal("uexpected error missing var should be ignored")
	}
}

func equal(got, e interface{}) bool {
	switch exp := e.(type) {
	case []interface{}:
		gotL, ok := got.([]interface{})
		if !ok {
			return false
		}
		if len(gotL) != len(exp) {
			return false
		}
		for i := range exp {
			if !equal(exp[i], gotL[i]) {
				return false
			}
		}
		return true
	case tick.Var:
		gotV, ok := got.(tick.Var)
		if !ok {
			return false
		}
		return exp.Type == gotV.Type && equal(exp.Value, gotV.Value)
	case []tick.Var:
		gotL, ok := got.([]tick.Var)
		if !ok {
			return false
		}
		if len(gotL) != len(exp) {
			return false
		}
		for i := range exp {
			if !equal(exp[i], gotL[i]) {
				return false
			}
		}
		return true
	case ast.Node:
		return exp.Equal(got)
	default:
		return reflect.DeepEqual(exp, got)
	}
}

func TestEvaluate_Vars_AllTypes(t *testing.T) {
	script := `
var str = 'this is a string'
var strA = str + ' concat'
var strZero string
var strCopy = str

var strList = [ 'string list', str, strA, strCopy, *]
var strListZero list
var strListCopy = strList

var star = *
var starZero star
var starCopy = star

var integer = 42
var intergerMath = (integer * 2) - ( 6 * 9 ) + (36 / 3)
var intZero int
var integerCopy= integer

var float = 3.14
var tastyPie = float * 42.0
var floatZero float
var floatCopy = float

var intFloat = int(sqrt(float)) * 3

var duration = 5m
var later = duration + 1m
var durationZero duration
var durationCopy = duration

var regex = /.*/
var regexZero regex
var regexCopy = regex

var boolean = TRUE
var f = boolean AND FALSE
var booleanZero bool
var booleanCopy = boolean

var lambda = lambda: "value" > 0
var l = lambda: lambda OR "value" < -100
var lambdaZero lambda
var lambdaCopy = lambda
`

	scope := stateful.NewScope()
	vars, err := tick.Evaluate(script, scope, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	expLambda := &ast.LambdaNode{
		Expression: &ast.BinaryNode{
			Operator: ast.TokenGreater,
			Left: &ast.ReferenceNode{
				Reference: "value",
			},
			Right: &ast.NumberNode{
				IsInt: true,
				Int64: 0,
			},
		},
	}
	expNestedLambda := &ast.LambdaNode{
		Expression: &ast.BinaryNode{
			Operator: ast.TokenOr,
			Left:     expLambda,
			Right: &ast.BinaryNode{
				Operator: ast.TokenLess,
				Left: &ast.ReferenceNode{
					Reference: "value",
				},
				Right: &ast.UnaryNode{
					Operator: ast.TokenMinus,
					Node: &ast.NumberNode{
						IsInt: true,
						Int64: 100,
					},
				},
			},
		},
	}

	expStrList := []interface{}{
		"string list",
		"this is a string",
		"this is a string concat",
		"this is a string",
		&ast.StarNode{},
	}
	expStrVarList := tick.Var{
		Value: []tick.Var{
			{Value: "string list", Type: ast.TString},
			{Value: "this is a string", Type: ast.TString},
			{Value: "this is a string concat", Type: ast.TString},
			{Value: "this is a string", Type: ast.TString},
			{Value: &ast.StarNode{}, Type: ast.TStar},
		},
		Type: ast.TList,
	}

	expScope := map[string]interface{}{
		"str":          "this is a string",
		"strCopy":      "this is a string",
		"strA":         "this is a string concat",
		"strZero":      "",
		"strList":      expStrList,
		"strListCopy":  expStrList,
		"strListZero":  []interface{}(nil),
		"star":         &ast.StarNode{},
		"starZero":     (*ast.StarNode)(nil),
		"starCopy":     &ast.StarNode{},
		"integer":      int64(42),
		"integerCopy":  int64(42),
		"intergerMath": int64(42),
		"intZero":      int64(0),
		"float":        3.14,
		"floatCopy":    3.14,
		"tastyPie":     3.14 * 42.0,
		"intFloat":     int64(3),
		"floatZero":    float64(0),
		"duration":     5 * time.Minute,
		"durationCopy": 5 * time.Minute,
		"later":        6 * time.Minute,
		"durationZero": time.Duration(0),
		"regex":        regexp.MustCompile(".*"),
		"regexCopy":    regexp.MustCompile(".*"),
		"regexZero":    (*regexp.Regexp)(nil),
		"boolean":      true,
		"booleanCopy":  true,
		"f":            false,
		"booleanZero":  false,
		"lambda":       expLambda,
		"lambdaCopy":   expLambda,
		"l":            expNestedLambda,
		"lambdaZero":   (*ast.LambdaNode)(nil),
	}
	expVars := map[string]tick.Var{
		"strList":     expStrVarList,
		"strListCopy": expStrVarList,
	}

	for name, value := range expScope {
		if got, err := scope.Get(name); err != nil {
			t.Errorf("unexpected error for %s: %s", name, err)
		} else if !equal(got, value) {
			t.Errorf("unexpected %s value: \ngot\n%v\ngot type: %T\nexp\n%v\nexp type: %T", name, got, got, value, value)
		}
		expVar, ok := expVars[name]
		if !ok {
			typ := ast.TypeOf(value)
			if strings.Contains(name, "Zero") {
				value = interface{}(nil)
			}
			expVar = tick.Var{
				Type:  typ,
				Value: value,
			}
		}
		if got, exp := vars[name].Value, expVar.Value; !equal(exp, got) {
			t.Errorf("unexpected %s var value:\ngot\n%v\nexp\n%v\n", name, got, exp)
		}
		if got, exp := vars[name].Type, expVar.Type; got != exp {
			t.Errorf("unexpected %s var type: got %v exp %v", name, got, exp)
		}
	}
}

func TestEvaluate_Vars_Immutable(t *testing.T) {
	script := `
var x = 3m
var x = 2m
`

	scope := stateful.NewScope()
	_, err := tick.Evaluate(script, scope, nil, false)
	if exp, got := "attempted to redefine x, vars are immutable", err.Error(); exp != got {
		t.Errorf("unexpected error message: got %s exp %s", got, exp)
	}

	x, err := scope.Get("x")
	if err != nil {
		t.Fatal(err)
	}
	if value, ok := x.(time.Duration); ok {
		if exp, got := time.Minute*3, value; exp != got {
			t.Errorf("unexpected x value: exp %v got %v", exp, got)
		}
	} else {
		t.Errorf("unexpected x value type: exp time.Duration got %T", x)
	}
}

func TestEvaluate_Vars_TypeConversion(t *testing.T) {
	script := `
var d = 5m
var messageDuration = '{{ .ID }} has crossed threshold: ' + string(d)

var x = 5
var messageInt = '{{ .ID }} has crossed threshold: ' + string(x)

var y = 1.0 / 3.0
var messageFloat = '{{ .ID }} has crossed threshold: ' + string(y)

var z = FALSE
var messageBool = '{{ .ID }} is: ' + string(z)

`

	scope := stateful.NewScope()
	vars, err := tick.Evaluate(script, scope, nil, true)
	if err != nil {
		t.Fatal(err)
	}

	exp := map[string]interface{}{
		"d":               5 * time.Minute,
		"messageDuration": "{{ .ID }} has crossed threshold: 5m",
		"x":               int64(5),
		"messageInt":      "{{ .ID }} has crossed threshold: 5",
		"y":               1.0 / 3.0,
		"messageFloat":    "{{ .ID }} has crossed threshold: 0.3333333333333333",
		"z":               false,
		"messageBool":     "{{ .ID }} is: false",
	}

	for name, value := range exp {
		if got, err := scope.Get(name); err != nil {
			t.Errorf("unexpected error for %s: %s", name, err)
		} else if !reflect.DeepEqual(got, value) {
			t.Errorf("unexpected %s value: got %v exp %v", name, got, value)
		}
		if got, exp := vars[name].Value, value; !reflect.DeepEqual(got, exp) {
			t.Errorf("unexpected %s vars value: got %v exp %v", name, got, value)
		}
		if got, exp := vars[name].Type, ast.TypeOf(value); got != exp {
			t.Errorf("unexpected %s vars type: got %v exp %v", name, got, exp)
		}
	}
}

// Test that using the wrong chain operator fails
func TestStrictEvaluate(t *testing.T) {
	script := `
var s2 = a.structB()
			.field1('f1')
			.field2(42)
`

	scope := stateful.NewScope()
	a := &structA{}
	scope.Set("a", a)

	_, err := tick.Evaluate(script, scope, nil, false)
	if err == nil {
		t.Fatal("expected error from Evaluate")
	}
}

func TestEvaluate_ListVars(t *testing.T) {
	script := `
var strList = [ 'host', 'dc', 'service' ]
f(strList)
`

	called := false
	f := func(a, b, c string) interface{} {
		called = true
		got := []string{a, b, c}
		exp := []string{"host", "dc", "service"}
		if !reflect.DeepEqual(got, exp) {
			t.Errorf("unexpected func args got %v exp %v", got, exp)
		}
		return nil
	}
	scope := stateful.NewScope()
	scope.Set("f", f)

	_, err := tick.Evaluate(script, scope, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("expected function to be called")
	}
}

func TestEvaluate_StringQuotesError(t *testing.T) {
	script := `
f("asdf")
`

	f := func(got string) (interface{}, error) {
		return nil, errors.New("function should not be called")
	}
	scope := stateful.NewScope()
	scope.Set("f", f)

	_, err := tick.Evaluate(script, scope, nil, false)
	if err == nil {
		t.Fatal("expected error from invalid string call")
	} else if got, exp := err.Error(), "line 2 char 1: cannot assign *ast.ReferenceNode to type string, did you use double quotes instead of single quotes?"; got != exp {
		t.Errorf("unexpected error string: \ngot\n%s\nexp\n%s\n", got, exp)
	}
}

func TestEvaluate_Func_Expression_Parameter(t *testing.T) {
	script := `
f('asdf' + string(10) + 'qwerty')
`

	f := func(got string) (interface{}, error) {
		if exp := "asdf10qwerty"; got != exp {
			t.Errorf("unexpected arg to function: got %s exp %s", got, exp)
		}
		return nil, nil
	}
	scope := stateful.NewScope()
	scope.Set("f", f)

	if _, err := tick.Evaluate(script, scope, nil, false); err != nil {
		t.Fatal(err)
	}
}

//------------------------------------
// Types for TestReflectionDescriber
//

// Basic type
type A struct {
	AProperty               string
	AFlag                   bool `tick:"PropertyMethodA"`
	AHiddenPMFlag           bool `tick:"HiddenPropertyMethod"`
	propertyMethodCallCount int
	chainMethodCallCount    int
	hcmCallCount            int
}

func (a *A) PropertyMethodA() *A {
	a.AFlag = true
	a.propertyMethodCallCount++
	return a
}

func (a *A) ChainMethodA() *A {
	a.chainMethodCallCount++
	return new(A)
}

func (a *A) HiddenPropertyMethod() *A {
	a.AHiddenPMFlag = true
	return a
}

func (a *A) HiddenChainMethod() *A {
	a.hcmCallCount++
	return new(A)
}

func (a *A) privateMethod() {}

// Type that embeds A
type B struct {
	A
	BProperty        string
	BFlag            bool   `tick:"PropertyMethodB"`
	BOverriddingProp string `tick:"HiddenChainMethod"`

	// Property hiding ChainMethodA
	ChainMethodA string

	propertyMethodCallCount int
	chainMethodCallCount    int
	apCallCount             int
	hpmCallCount            int
}

func (b *B) PropertyMethodB() *B {
	b.BFlag = true
	b.propertyMethodCallCount++
	return b
}

func (b *B) ChainMethodB() *B {
	b.chainMethodCallCount++
	return new(B)
}

// Chain method hiding AProperty
func (b *B) AProperty() *B {
	b.apCallCount++
	return new(B)
}

// Chain method hiding A.HiddenPropertyMethod property method
func (b *B) HiddenPropertyMethod() *B {
	b.hpmCallCount++
	return new(B)
}

// Property method hidding chain method
func (b *B) HiddenChainMethod(value string) *B {
	b.BOverriddingProp = value
	return b
}

// Type that embeds *A
type C struct {
	*A
	CProperty        string
	CFlag            bool   `tick:"PropertyMethodC"`
	COverriddingProp string `tick:"HiddenChainMethod"`

	// Property hiding ChainMethodA
	ChainMethodA string

	propertyMethodCallCount int
	chainMethodCallCount    int
	apCallCount             int
	hpmCallCount            int
}

func (c *C) PropertyMethodC() *C {
	c.CFlag = true
	c.propertyMethodCallCount++
	return c
}

func (c *C) ChainMethodC() (*C, error) {
	c.chainMethodCallCount++
	return new(C), nil
}

// Chain method hiding AProperty
func (c *C) AProperty() *C {
	c.apCallCount++
	return new(C)
}

// Chain method hiding A.HiddenPropertyMethod property method
func (c *C) HiddenPropertyMethod() *C {
	c.hpmCallCount++
	return new(C)
}

// Property method hidding chain method
func (c *C) HiddenChainMethod(value string) *C {
	c.COverriddingProp = value
	return c
}

func TestReflectionDescriber(t *testing.T) {
	//----------------
	// Test A type
	//
	a := new(A)
	rdA, err := tick.NewReflectionDescriber(a, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Test A.privateMethod
	if exp, got := false, rdA.HasProperty("privateMethod"); exp != got {
		t.Fatalf("unexpected HasProperty got: %v exp: %v", got, exp)
	}

	// Test A.AProperty
	if exp, got := true, rdA.HasProperty("aProperty"); exp != got {
		t.Fatalf("unexpected HasProperty got: %v exp: %v", got, exp)
	}
	if exp, got := false, rdA.HasChainMethod("aProperty"); exp != got {
		t.Fatalf("unexpected HasChainMethod got: %v exp: %v", got, exp)
	}
	_, err = rdA.SetProperty("aProperty", "test")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := "test", a.AProperty; exp != got {
		t.Fatalf("unexpected a.AProperty got: %v exp: %v", got, exp)
	}

	// Test A.PropertyMethodA
	if exp, got := true, rdA.HasProperty("propertyMethodA"); exp != got {
		t.Fatalf("unexpected HasProperty got: %v exp: %v", got, exp)
	}
	if exp, got := false, rdA.HasChainMethod("propertyMethodA"); exp != got {
		t.Fatalf("unexpected HasChainMethod got: %v exp: %v", got, exp)
	}
	if exp, got := false, rdA.HasProperty("aFlag"); exp != got {
		t.Fatalf("unexpected HasProperty got: %v exp: %v", got, exp)
	}
	_, err = rdA.SetProperty("propertyMethodA")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := true, a.AFlag; exp != got {
		t.Fatalf("unexpected a.AFlag got: %v exp: %v", got, exp)
	}
	if exp, got := 1, a.propertyMethodCallCount; exp != got {
		t.Fatalf("unexpected a.propertyMethodCallCount got: %v exp: %v", got, exp)
	}

	// Test A.HiddenPropertyMethod
	if exp, got := true, rdA.HasProperty("hiddenPropertyMethod"); exp != got {
		t.Fatalf("unexpected HasProperty got: %v exp: %v", got, exp)
	}
	if exp, got := false, rdA.HasChainMethod("hiddenPropertyMethod"); exp != got {
		t.Fatalf("unexpected HasChainMethod got: %v exp: %v", got, exp)
	}
	if exp, got := false, rdA.HasProperty("aHiddenPMFlag"); exp != got {
		t.Fatalf("unexpected HasProperty got: %v exp: %v", got, exp)
	}
	_, err = rdA.SetProperty("hiddenPropertyMethod")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := true, a.AHiddenPMFlag; exp != got {
		t.Fatalf("unexpected a.AHiddenPMFlag got: %v exp: %v", got, exp)
	}

	// Test A.ChainMethodA
	if exp, got := true, rdA.HasChainMethod("chainMethodA"); exp != got {
		t.Fatalf("unexpected HasChainMethod got: %v exp: %v", got, exp)
	}
	if exp, got := false, rdA.HasProperty("chainMethodA"); exp != got {
		t.Fatalf("unexpected HasProperty got: %v exp: %v", got, exp)
	}
	_, err = rdA.CallChainMethod("chainMethodA")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := 1, a.chainMethodCallCount; exp != got {
		t.Fatalf("unexpected a.chainMethodCallCount got: %v exp: %v", got, exp)
	}

	// Test A.HiddenChainMethod
	if exp, got := true, rdA.HasChainMethod("hiddenChainMethod"); exp != got {
		t.Fatalf("unexpected HasChainMethod got: %v exp: %v", got, exp)
	}
	if exp, got := false, rdA.HasProperty("hiddenChainMethod"); exp != got {
		t.Fatalf("unexpected HasProperty got: %v exp: %v", got, exp)
	}
	_, err = rdA.CallChainMethod("hiddenChainMethod")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := 1, a.hcmCallCount; exp != got {
		t.Fatalf("unexpected a.hcmCallCount got: %v exp: %v", got, exp)
	}

	//----------------
	// Test B type
	//
	b := new(B)
	rdB, err := tick.NewReflectionDescriber(b, map[string]reflect.Value{
		"HiddenPropertyMethod": reflect.ValueOf(b.HiddenPropertyMethod),
		"HiddenChainMethod":    reflect.ValueOf(b.A.HiddenChainMethod),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Test B.AProperty as property
	if exp, got := true, rdB.HasProperty("aProperty"); exp != got {
		t.Fatalf("unexpected HasProperty got: %v exp: %v", got, exp)
	}
	_, err = rdB.SetProperty("aProperty", "test")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := "test", b.A.AProperty; exp != got {
		t.Fatalf("unexpected b.A.AProperty got: %v exp: %v", got, exp)
	}

	// Test B.AProperty as chain
	if exp, got := true, rdB.HasChainMethod("aProperty"); exp != got {
		t.Fatalf("unexpected HasChainMethod got: %v exp: %v", got, exp)
	}
	_, err = rdB.CallChainMethod("aProperty")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := 1, b.apCallCount; exp != got {
		t.Fatalf("unexpected b.apCallCount got: %v exp: %v", got, exp)
	}

	// Test B.PropertyMethodA
	if exp, got := true, rdB.HasProperty("propertyMethodA"); exp != got {
		t.Fatalf("unexpected HasProperty got: %v exp: %v", got, exp)
	}
	if exp, got := false, rdB.HasChainMethod("propertyMethodA"); exp != got {
		t.Fatalf("unexpected HasChainMethod got: %v exp: %v", got, exp)
	}
	if exp, got := false, rdB.HasProperty("aFlag"); exp != got {
		t.Fatalf("unexpected HasProperty got: %v exp: %v", got, exp)
	}
	_, err = rdB.SetProperty("propertyMethodA")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := true, b.AFlag; exp != got {
		t.Fatalf("unexpected b.AFlag got: %v exp: %v", got, exp)
	}
	if exp, got := 1, b.A.propertyMethodCallCount; exp != got {
		t.Fatalf("unexpected b.A.propertyMethodCallCount got: %v exp: %v", got, exp)
	}

	// Test B.HiddenPropertyMethod as chain
	if exp, got := true, rdB.HasChainMethod("hiddenPropertyMethod"); exp != got {
		t.Fatalf("unexpected HasChainMethod got: %v exp: %v", got, exp)
	}
	_, err = rdB.CallChainMethod("hiddenPropertyMethod")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := 1, b.hpmCallCount; exp != got {
		t.Fatalf("unexpected b.hpmCallCount got: %v exp: %v", got, exp)
	}

	// Test B.HiddenPropertyMethod as property
	if exp, got := true, rdB.HasProperty("hiddenPropertyMethod"); exp != got {
		t.Fatalf("unexpected HasProperty got: %v exp: %v", got, exp)
	}
	_, err = rdB.SetProperty("hiddenPropertyMethod")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := true, b.AHiddenPMFlag; exp != got {
		t.Fatalf("unexpected b.AHiddenPMFlag got: %v exp: %v", got, exp)
	}

	// Test B.HiddenChainMethod as chain
	if exp, got := true, rdB.HasChainMethod("hiddenChainMethod"); exp != got {
		t.Fatalf("unexpected HasChainMethod got: %v exp: %v", got, exp)
	}
	_, err = rdB.CallChainMethod("hiddenChainMethod")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := 1, b.A.hcmCallCount; exp != got {
		t.Fatalf("unexpected b.A.hcmCallCount got: %v exp: %v", got, exp)
	}

	// Test B.HiddenPropertyMethod as property
	if exp, got := true, rdB.HasProperty("hiddenChainMethod"); exp != got {
		t.Fatalf("unexpected HasProperty got: %v exp: %v", got, exp)
	}
	_, err = rdB.SetProperty("hiddenChainMethod", "test")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := "test", b.BOverriddingProp; exp != got {
		t.Fatalf("unexpected b.BOverriddingProp got: %v exp: %v", got, exp)
	}

	// Test B.ChainMethodA as chain
	if exp, got := true, rdB.HasChainMethod("chainMethodA"); exp != got {
		t.Fatalf("unexpected HasChainMethod got: %v exp: %v", got, exp)
	}
	_, err = rdB.CallChainMethod("chainMethodA")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := 1, b.A.chainMethodCallCount; exp != got {
		t.Fatalf("unexpected b.A.chainMethodCallCount got: %v exp: %v", got, exp)
	}

	// Test B.ChainMethodA as property
	if exp, got := true, rdB.HasProperty("chainMethodA"); exp != got {
		t.Fatalf("unexpected HasProperty got: %v exp: %v", got, exp)
	}
	_, err = rdB.SetProperty("chainMethodA", "test")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := "test", b.ChainMethodA; exp != got {
		t.Fatalf("unexpected b.ChainMethodA got: %v exp: %v", got, exp)
	}

	// Test B.BProperty
	if exp, got := true, rdB.HasProperty("bProperty"); exp != got {
		t.Fatalf("unexpected HasProperty got: %v exp: %v", got, exp)
	}
	if exp, got := false, rdB.HasChainMethod("bProperty"); exp != got {
		t.Fatalf("unexpected HasChainMethod got: %v exp: %v", got, exp)
	}
	_, err = rdB.SetProperty("bProperty", "test")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := "test", b.BProperty; exp != got {
		t.Fatalf("unexpected b.BProperty got: %v exp: %v", got, exp)
	}

	// Test B.PropertyMethodB
	if exp, got := true, rdB.HasProperty("propertyMethodB"); exp != got {
		t.Fatalf("unexpected HasProperty got: %v exp: %v", got, exp)
	}
	if exp, got := false, rdB.HasChainMethod("propertyMethodB"); exp != got {
		t.Fatalf("unexpected HasChainMethod got: %v exp: %v", got, exp)
	}
	_, err = rdB.SetProperty("propertyMethodB")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := true, b.BFlag; exp != got {
		t.Fatalf("unexpected b.BFlag got: %v exp: %v", got, exp)
	}
	if exp, got := 1, b.propertyMethodCallCount; exp != got {
		t.Fatalf("unexpected b.propertyMethodCallCount got: %v exp: %v", got, exp)
	}

	// Test B.ChainMethodB
	if exp, got := true, rdB.HasChainMethod("chainMethodB"); exp != got {
		t.Fatalf("unexpected HasChainMethod got: %v exp: %v", got, exp)
	}
	if exp, got := false, rdB.HasProperty("chainMethodB"); exp != got {
		t.Fatalf("unexpected HasProperty got: %v exp: %v", got, exp)
	}
	_, err = rdB.CallChainMethod("chainMethodB")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := 1, b.chainMethodCallCount; exp != got {
		t.Fatalf("unexpected b.chainMethodCallCount got: %v exp: %v", got, exp)
	}

	//----------------
	// Test C type
	//
	c := &C{
		A: new(A),
	}
	rdC, err := tick.NewReflectionDescriber(c, map[string]reflect.Value{
		"HiddenPropertyMethod": reflect.ValueOf(c.HiddenPropertyMethod),
		"HiddenChainMethod":    reflect.ValueOf(c.A.HiddenChainMethod),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Test C.AProperty as property
	if exp, got := true, rdC.HasProperty("aProperty"); exp != got {
		t.Fatalf("unexpected HasProperty got: %v exp: %v", got, exp)
	}
	_, err = rdC.SetProperty("aProperty", "test")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := "test", c.A.AProperty; exp != got {
		t.Fatalf("unexpected c.A.AProperty got: %v exp: %v", got, exp)
	}

	// Test C.AProperty as chain
	if exp, got := true, rdC.HasChainMethod("aProperty"); exp != got {
		t.Fatalf("unexpected HasChainMethod got: %v exp: %v", got, exp)
	}
	_, err = rdC.CallChainMethod("aProperty")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := 1, c.apCallCount; exp != got {
		t.Fatalf("unexpected c.apCallCount got: %v exp: %v", got, exp)
	}

	// Test C.PropertyMethodA
	if exp, got := true, rdC.HasProperty("propertyMethodA"); exp != got {
		t.Fatalf("unexpected HasProperty got: %v exp: %v", got, exp)
	}
	if exp, got := false, rdC.HasChainMethod("propertyMethodA"); exp != got {
		t.Fatalf("unexpected HasChainMethod got: %v exp: %v", got, exp)
	}
	if exp, got := false, rdC.HasProperty("aFlag"); exp != got {
		t.Fatalf("unexpected HasProperty got: %v exp: %v", got, exp)
	}
	_, err = rdC.SetProperty("propertyMethodA")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := true, c.AFlag; exp != got {
		t.Fatalf("unexpected c.AFlag got: %v exp: %v", got, exp)
	}
	if exp, got := 1, c.A.propertyMethodCallCount; exp != got {
		t.Fatalf("unexpected c.A.propertyMethodCallCount got: %v exp: %v", got, exp)
	}

	// Test C.HiddenPropertyMethod as chain
	if exp, got := true, rdC.HasChainMethod("hiddenPropertyMethod"); exp != got {
		t.Fatalf("unexpected HasChainMethod got: %v exp: %v", got, exp)
	}
	_, err = rdC.CallChainMethod("hiddenPropertyMethod")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := 1, c.hpmCallCount; exp != got {
		t.Fatalf("unexpected c.hpmCallCount got: %v exp: %v", got, exp)
	}

	// Test C.HiddenPropertyMethod as property
	if exp, got := true, rdC.HasProperty("hiddenPropertyMethod"); exp != got {
		t.Fatalf("unexpected HasProperty got: %v exp: %v", got, exp)
	}
	_, err = rdC.SetProperty("hiddenPropertyMethod")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := true, c.AHiddenPMFlag; exp != got {
		t.Fatalf("unexpected c.AHiddenPMFlag got: %v exp: %v", got, exp)
	}

	// Test C.HiddenChainMethod as chain
	if exp, got := true, rdC.HasChainMethod("hiddenChainMethod"); exp != got {
		t.Fatalf("unexpected HasChainMethod got: %v exp: %v", got, exp)
	}
	_, err = rdC.CallChainMethod("hiddenChainMethod")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := 1, c.A.hcmCallCount; exp != got {
		t.Fatalf("unexpected c.A.hcmCallCount got: %v exp: %v", got, exp)
	}

	// Test C.HiddenPropertyMethod as property
	if exp, got := true, rdC.HasProperty("hiddenChainMethod"); exp != got {
		t.Fatalf("unexpected HasProperty got: %v exp: %v", got, exp)
	}
	_, err = rdC.SetProperty("hiddenChainMethod", "test")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := "test", c.COverriddingProp; exp != got {
		t.Fatalf("unexpected c.COverriddingProp got: %v exp: %v", got, exp)
	}

	// Test C.ChainMethodA as chain
	if exp, got := true, rdC.HasChainMethod("chainMethodA"); exp != got {
		t.Fatalf("unexpected HasChainMethod got: %v exp: %v", got, exp)
	}
	_, err = rdC.CallChainMethod("chainMethodA")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := 1, c.A.chainMethodCallCount; exp != got {
		t.Fatalf("unexpected c.A.chainMethodCallCount got: %v exp: %v", got, exp)
	}

	// Test C.ChainMethodA as property
	if exp, got := true, rdC.HasProperty("chainMethodA"); exp != got {
		t.Fatalf("unexpected HasProperty got: %v exp: %v", got, exp)
	}
	_, err = rdC.SetProperty("chainMethodA", "test")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := "test", c.ChainMethodA; exp != got {
		t.Fatalf("unexpected c.ChainMethodA got: %v exp: %v", got, exp)
	}

	// Test C.CProperty
	if exp, got := true, rdC.HasProperty("cProperty"); exp != got {
		t.Fatalf("unexpected HasProperty got: %v exp: %v", got, exp)
	}
	if exp, got := false, rdC.HasChainMethod("cProperty"); exp != got {
		t.Fatalf("unexpected HasChainMethod got: %v exp: %v", got, exp)
	}
	_, err = rdC.SetProperty("cProperty", "test")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := "test", c.CProperty; exp != got {
		t.Fatalf("unexpected c.CProperty got: %v exp: %v", got, exp)
	}

	// Test C.PropertyMethodC
	if exp, got := true, rdC.HasProperty("propertyMethodC"); exp != got {
		t.Fatalf("unexpected HasProperty got: %v exp: %v", got, exp)
	}
	if exp, got := false, rdC.HasChainMethod("propertyMethodC"); exp != got {
		t.Fatalf("unexpected HasChainMethod got: %v exp: %v", got, exp)
	}
	_, err = rdC.SetProperty("propertyMethodC")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := true, c.CFlag; exp != got {
		t.Fatalf("unexpected c.CFlag got: %v exp: %v", got, exp)
	}
	if exp, got := 1, c.propertyMethodCallCount; exp != got {
		t.Fatalf("unexpected c.propertyMethodCallCount got: %v exp: %v", got, exp)
	}

	// Test C.ChainMethodC
	if exp, got := true, rdC.HasChainMethod("chainMethodC"); exp != got {
		t.Fatalf("unexpected HasChainMethod got: %v exp: %v", got, exp)
	}
	if exp, got := false, rdC.HasProperty("chainMethodC"); exp != got {
		t.Fatalf("unexpected HasProperty got: %v exp: %v", got, exp)
	}
	_, err = rdC.CallChainMethod("chainMethodC")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := 1, c.chainMethodCallCount; exp != got {
		t.Fatalf("unexpected c.chainMethodCallCount got: %v exp: %v", got, exp)
	}
}

func TestReflectionDescriberErrors(t *testing.T) {
	_, err := tick.NewReflectionDescriber(nil, nil)
	if err == nil {
		t.Error("expected err got nil")
	}

	o := struct{}{}
	_, err = tick.NewReflectionDescriber(o, nil)
	if err == nil {
		t.Error("expected err got nil")
	}

	var c *C
	_, err = tick.NewReflectionDescriber(c, nil)
	if err == nil {
		t.Error("expected err got nil")
	}

	c = new(C)
	_, err = tick.NewReflectionDescriber(c, nil)
	if err == nil {
		t.Error("expected err got nil")
	}

	i := new(int)
	*i = 42
	_, err = tick.NewReflectionDescriber(i, nil)
	if err == nil {
		t.Error("expected err got nil")
	}

}
