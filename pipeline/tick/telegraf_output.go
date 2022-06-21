package tick

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/influxdata/telegraf/config"
	"reflect"
	"time"

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

func (n *TelegrafNode) Build(pipe *pipeline.TelegrafOutNode) (ast.Node, error) {
	p := n.Pipe("telegraf")
	for k, c := range outputs.Outputs {
		p.Dot(snaker.SnakeToCamel(k))
		processStruct(p, k, reflect.ValueOf(c()).Elem())
	}
	return n.prev, n.err
}

func processStruct(p *Function, k string, v reflect.Value) {

	for _, field := range reflect.VisibleFields(v.Type()) { // embedded fields are treated as part of the outer struct
		// fmt.Println(i, field.Tag)
		tagVal, _ := field.Tag.Lookup("toml")
		if tagVal != "-" && tagVal != "_" && field.Name != "_" && field.IsExported() {
			//if k != "kafka" {
			//	return
			//}
			//fmt.Println(k, field.Name, tagVal)
			funcName := tagVal
			if funcName == "" {
				funcName = field.Name
			}
			processField(p, k, v.FieldByName(field.Name), snaker.SnakeToCamel(funcName))
		}
	}

}

func processField(p *Function, k string, f reflect.Value, camelName string) {
	_, ok := (f.Interface()).(toml.Unmarshaler)
	switch {
	case f.IsZero():
	case f.Kind() == reflect.Pointer:
		processField(p, k, reflect.Indirect(f), camelName)
	case f.Type() == reflect.TypeOf(config.Duration(0)):
		p.Dot(camelName, f.Convert(reflect.TypeOf(time.Duration(0))).Interface())
	case f.Type() == reflect.TypeOf(config.Size(0)):
		p.Dot(camelName, f.Convert(reflect.TypeOf("")).Interface())
	case ok:
		panic("not implemented")
	case f.Type() == reflect.TypeOf([]string(nil)):
		//TODO: implement []string
		p.Dot(camelName)
	case f.Kind() == reflect.Map:
		//TODO: implement map
		p.Dot(camelName)
	case f.Kind() == reflect.Func:
		// skip funcs
	case f.Type() == reflect.TypeOf(uint8(0)):
		p.Dot(camelName, f.Convert(reflect.TypeOf(int64(0))).Interface())
	case f.Type() == reflect.TypeOf(int(0)):
		p.Dot(camelName, f.Convert(reflect.TypeOf(int64(0))).Interface())

	case f.Kind() == reflect.Struct:
		processStruct(p, k, f)
	case f.Kind() == reflect.Interface:
		// if the kind is interface, then we can't marshall into it.
	default:
		fmt.Println("hit default", k, f.Interface(), f.Kind(), f.Type())
		p.Dot(camelName, f.Interface())
	}
}

//}
