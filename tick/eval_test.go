package tick_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/influxdata/kapacitor/tick"
	"github.com/stretchr/testify/assert"
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
	field1  string
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
	assert := assert.New(t)

	//Run a test that evaluates the DSL against the above structures.
	script := `
var s2 = a.structB()
			.field1('f1')
			.field2(42)

s2.field3(15m)

s2.structC()
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
	assert.NotNil(s2)
	assert.Equal("f1", s2.Field1)
	assert.Equal(int64(42), s2.Field2)
	assert.Equal(time.Minute*15, s2.Field3)

	s3 := s2.c
	if assert.NotNil(s3) {
		assert.Equal("c", s3.field1)
		assert.Equal(21.5, s3.field2)
		assert.Equal(time.Hour*7, s3.field3)
		if assert.NotNil(s3.AggFunc) {
			assert.Equal([]float64{10.0}, s3.AggFunc([]float64{5, 5}))
		}
	}
}

func TestEvaluate_DynamicMethod(t *testing.T) {
	script := `var x = a.dynamicMethod(1,'str', 10s).sad(FALSE)`

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
