package tick_test

import (
	"testing"
	"time"

	"github.com/influxdb/kapacitor/tick"
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
	Agg agg
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

	i := &influxql{}
	i.Agg.Sum = aggSum
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
