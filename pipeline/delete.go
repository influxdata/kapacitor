package pipeline

// Deletes fields and tags from data points.
//
// Example:
//    stream
//        |delete()
//            .field('value')
//            .tag('host')
//
// The above example will remove the field `value` and the tag `host`, from each point.
//
// Available Statistics:
//
//    * fields_deleted -- number of fields that were deleted. Only counts if the field already existed.
//    * tags_deleted -- number of tags that were deleted. Only counts if the tag already existed.
//
type DeleteNode struct {
	chainnode

	// Set of fields to delete
	// tick:ignore
	Fields []string `tick:"Field"`

	// Set of tags to delete
	// tick:ignore
	Tags []string `tick:"Tag"`
}

func newDeleteNode(e EdgeType) *DeleteNode {
	n := &DeleteNode{
		chainnode: newBasicChainNode("delete", e, e),
	}
	return n
}

// Delete a field.
// tick:property
func (n *DeleteNode) Field(name string) *DeleteNode {
	n.Fields = append(n.Fields, name)
	return n
}

// Delete a tag.
// tick:property
func (n *DeleteNode) Tag(name string) *DeleteNode {
	n.Tags = append(n.Tags, name)
	return n
}
