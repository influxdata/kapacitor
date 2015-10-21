package pipeline

// Applies a transformation to each data point it receives.
// The transformation function can be defined using the built-in
// `expr` function.
//
// Example:
//    stream
//        .apply(expr("error_percent", "error_count / total_count"))
//
// The above example will add a new field `error_percent` to each
// data point with the result of `error_count / total_count` where
// `error_count` and `total_count` are existing fields on the data point.
type ApplyNode struct {
	chainnode

	// tick:ignore
	Func interface{}
}

func newApplyNode(e EdgeType, f interface{}) *ApplyNode {
	a := &ApplyNode{
		chainnode: newBasicChainNode("apply", e, e),
		Func:      f,
	}
	return a
}
