package tick

import (
	"reflect"

	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
	"github.com/influxdata/telegraf/plugins/outputs"
	_ "github.com/influxdata/telegraf/plugins/outputs/all"
	"github.com/serenize/snaker"
)

type TelegrafNode struct {
	Function
}

// NewTelegraf creates a Derivative function builder
func NewTelegraf(parents []ast.Node) *TelegrafNode {
	return &TelegrafNode{
		Function{
			Parents: parents,
		},
	}
}

func (n *TelegrafNode) Build(pipe *pipeline.Pipeline) (ast.Node, error) {
	p := n.Pipe("telegraf")
	for k, v := range outputs.Outputs {
		p.Dot(snaker.SnakeToCamel(k))
		numFields := reflect.TypeOf(v).NumField()
		for i := 0; i < numFields; i++ {
			field := reflect.TypeOf(v).Field(i)
			tagVal, ok := field.Tag.Lookup("toml")
			if ok && tagVal != "" {
				f := reflect.ValueOf(v).Field(i)
				camelName := snaker.SnakeToCamel(tagVal)
				switch {
				case field.Type.Kind() == reflect.Bool:
					p.DotIf(camelName, f.Bool())
				default:
					p.Dot(camelName, f.Addr())
				}
			}
		}
	}
	return n.prev, n.err
}
