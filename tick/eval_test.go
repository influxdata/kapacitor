package tick_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/influxdata/kapacitor/tick"
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

	scope := tick.NewScope()
	a := &structA{}
	scope.Set("a", a)

	i := &influxql{
		Agg: &agg{
			Sum: aggSum,
		},
	}
	scope.Set("influxql", i)

	err := tick.Evaluate(script, scope)
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

	scope := tick.NewScope()
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

	err := tick.Evaluate(script, scope)
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

func TestEvaluate_Vars(t *testing.T) {
	script := `
var x = 3m
var y = -x

var n = TRUE 
var m = !n 
`

	scope := tick.NewScope()
	err := tick.Evaluate(script, scope)
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
		t.Errorf("unexpected y value type: exp time.Duration got %T", x)
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

}

// Test that using the wrong chain operator fails
func TestStrictEvaluate(t *testing.T) {
	// Skip test until DEPRECATED syntax is removed
	t.Skip()
	script := `
var s2 = a.structB()
			.field1('f1')
			.field2(42)
`

	scope := tick.NewScope()
	a := &structA{}
	scope.Set("a", a)

	err := tick.Evaluate(script, scope)
	if err == nil {
		t.Fatal("expected error from Evaluate")
	}
}
