package kapacitor

import (
	"github.com/yozora-hitagi/kapacitor/pipeline"
	"github.com/yozora-hitagi/kapacitor/tick"
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
