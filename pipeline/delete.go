package pipeline

import (
	"encoding/json"
	"fmt"
)

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
	chainnode `json:"-"`

	// Set of fields to delete
	// tick:ignore
	Fields []string `tick:"Field" json:"fields"`

	// Set of tags to delete
	// tick:ignore
	Tags []string `tick:"Tag" json:"tags"`
}

func newDeleteNode(e EdgeType) *DeleteNode {
	n := &DeleteNode{
		chainnode: newBasicChainNode("delete", e, e),
	}
	return n
}

// MarshalJSON converts DeleteNode to JSON
// tick:ignore
func (n *DeleteNode) MarshalJSON() ([]byte, error) {
	type Alias DeleteNode
	var raw = &struct {
		TypeOf
		*Alias
	}{
		TypeOf: TypeOf{
			Type: "delete",
			ID:   n.ID(),
		},
		Alias: (*Alias)(n),
	}
	return json.Marshal(raw)
}

// UnmarshalJSON converts JSON to an DeleteNode
// tick:ignore
func (n *DeleteNode) UnmarshalJSON(data []byte) error {
	type Alias DeleteNode
	var raw = &struct {
		TypeOf
		*Alias
	}{
		Alias: (*Alias)(n),
	}
	err := json.Unmarshal(data, raw)
	if err != nil {
		return err
	}
	if raw.Type != "delete" {
		return fmt.Errorf("error unmarshaling node %d of type %s as DeleteNode", raw.ID, raw.Type)
	}
	n.setID(raw.ID)
	return nil
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
