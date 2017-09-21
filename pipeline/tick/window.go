package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
)

// Window converts the window pipeline node into the TICKScript AST
func (a *AST) Window(w *pipeline.WindowNode) *AST {
	// TODO: Handle the err
	window, _ := PipeFunction(a.Node, "window")
	window, _ = DotFunction(window, "period", w.Period)
	window, _ = DotFunction(window, "every", w.Every)
	window, _ = DotFunction(window, "periodCount", w.PeriodCount)
	window, _ = DotFunction(window, "everyCount", w.EveryCount)
	window, _ = DotFunctionIf(window, "align", w.AlignFlag)
	window, _ = DotFunctionIf(window, "fillPeriod", w.FillPeriodFlag)

	a.Node = window
	return a
}
