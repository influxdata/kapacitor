package dsl_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/influxdb/kapacitor/dsl"
	"github.com/stretchr/testify/assert"
)

//Test structure for evaluating a DSL

type structA struct {
	s *structB
}

type structB struct {
	Field1 string
	Field2 uint64
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
			.field1("f1")
			.field2(42);

s2.field3(15m);

s2.structC()
	.options("c", 21.5, 7h)
	.aggFunc(influxql.agg.sum);
`

	scope := dsl.NewScope()
	a := &structA{}
	scope.Set("a", a)

	i := &influxql{}
	i.Agg.Sum = aggSum
	scope.Set("influxql", i)

	err := dsl.Evaluate(script, scope)
	if !assert.Nil(err) {
		fmt.Println(err)
	}

	s2, ok := scope.Get("s2").(*structB)
	assert.NotNil(s2)
	if assert.True(ok, "s2 is not a *structB %q", s2) {
		assert.Equal("f1", s2.Field1)
		assert.Equal(uint64(42), s2.Field2)
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

}
