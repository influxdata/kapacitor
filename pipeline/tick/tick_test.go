package tick_test

import (
	"bytes"

	"github.com/influxdata/kapacitor/pipeline/tick"
	"github.com/influxdata/kapacitor/tick/ast"
)

var _ ast.Node = &NullNode{}

type NullNode struct {
	*tick.NullPosition
}

func (n *NullNode) String() string                     { return "" }
func (n *NullNode) Format(*bytes.Buffer, string, bool) {}
func (n *NullNode) Equal(o interface{}) (ok bool)      { _, ok = o.(*NullNode); return }
func (n *NullNode) MarshalJSON() ([]byte, error)       { return nil, nil }
