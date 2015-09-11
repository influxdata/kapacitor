package pipeline

import (
	"github.com/influxdb/kapacitor/dsl"
)

func CreatePipeline(dslScript string) (*Stream, error) {
	s := &Stream{}
	scope := dsl.NewScope()
	scope.Set("stream", s)
	err := dsl.Evaluate(dslScript, scope)
	return s, err
}
