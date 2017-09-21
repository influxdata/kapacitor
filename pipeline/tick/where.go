package tick

import "github.com/influxdata/kapacitor/pipeline"

// Where converts the where pipeline node into the TICKScript AST
func (a *AST) Where(w *pipeline.WhereNode) *AST {
	// TODO: Handle the err
	where, _ := PipeFunction(a.Node, "where", w.Lambda)
	a.Node = where
	return a
}
