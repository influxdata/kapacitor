package kapacitor

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick"
)

type Template struct {
	id string
	tp *pipeline.TemplatePipeline
}

func (t *Template) Vars() map[string]tick.Var {
	return t.tp.Vars()
}

func (t *Template) Dot() string {
	return string(t.tp.Dot(t.id))
}
