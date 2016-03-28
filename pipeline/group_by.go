package pipeline

// A GroupByNode will group the incoming data.
// Each group is then processed independently for the rest of the pipeline.
// Only tags that are dimensions in the grouping will be preserved;
// all other tags are dropped.
//
// Example:
//    stream
//        |groupBy('service', 'datacenter')
//        ...
//
// The above example groups the data along two dimensions `service` and `datacenter`.
// Groups are dynamically created as new data arrives and each group is processed
// independently.
type GroupByNode struct {
	chainnode
	//The dimensions by which to group to the data.
	// tick:ignore
	Dimensions []interface{}
}

func newGroupByNode(wants EdgeType, dims []interface{}) *GroupByNode {
	return &GroupByNode{
		chainnode:  newBasicChainNode("groupby", wants, wants),
		Dimensions: dims,
	}
}
