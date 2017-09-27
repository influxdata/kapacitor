package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// Window converts the window pipeline node into the TICKScript AST
type Window struct {
	Function
}

// Build creates a window ast.Node
func (n *Window) Build(w *pipeline.WindowNode) (ast.Node, error) {
	n.Pipe("window").
		Dot("period", w.Period).
		Dot("every", w.Every).
		Dot("periodCount", w.PeriodCount).
		Dot("everyCount", w.EveryCount).
		DotIf("align", w.AlignFlag).
		DotIf("fillPeriod", w.FillPeriodFlag)
	return n.prev, n.err
}
