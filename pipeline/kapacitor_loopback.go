package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Writes the data back into the Kapacitor stream.
// To write data to a remote Kapacitor instance use the InfluxDBOut node.
//
// Example:
//        |kapacitorLoopback()
//            .database('mydb')
//            .retentionPolicy('myrp')
//            .measurement('errors')
//            .tag('kapacitor', 'true')
//            .tag('version', '0.2')
//
//
// NOTE: It is possible to create infinite loops using this node.
// Take care to ensure you do not chain tasks together creating a loop.
//
// Available Statistics:
//
//    * points_written -- number of points written back to Kapacitor
//
type KapacitorLoopbackNode struct {
	node `json:"-"`

	// The name of the database.
	Database string `json:"database"`
	// The name of the retention policy.
	RetentionPolicy string `json:"retention_policy"`
	// The name of the measurement.
	Measurement string `json:"measurement"`
	// Static set of tags to add to all data points before writing them.
	// tick:ignore
	Tags map[string]string `tick:"Tag" json:"tags"`
}

func newKapacitorLoopbackNode(wants EdgeType) *KapacitorLoopbackNode {
	return &KapacitorLoopbackNode{
		node: node{
			desc:     "kapacitor_loopback",
			wants:    wants,
			provides: NoEdge,
		},
		Tags: make(map[string]string),
	}
}

// MarshalJSON converts KapacitorLoopbackNode to JSON
// tick:ignore
func (n *KapacitorLoopbackNode) MarshalJSON() ([]byte, error) {
	type Alias KapacitorLoopbackNode
	var raw = &struct {
		TypeOf
		*Alias
	}{
		TypeOf: TypeOf{
			Type: "kapacitorLoopback",
			ID:   n.ID(),
		},
		Alias: (*Alias)(n),
	}
	return json.Marshal(raw)
}

// UnmarshalJSON converts JSON to an KapacitorLoopbackNode
// tick:ignore
func (n *KapacitorLoopbackNode) UnmarshalJSON(data []byte) error {
	type Alias KapacitorLoopbackNode
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
	if raw.Type != "kapacitorLoopback" {
		return fmt.Errorf("error unmarshaling node %d of type %s as KapacitorLoopbackNode", raw.ID, raw.Type)
	}
	n.setID(raw.ID)
	return nil
}

// Add a static tag to all data points.
// Tag can be called more than once.
//
// tick:property
func (k *KapacitorLoopbackNode) Tag(key, value string) *KapacitorLoopbackNode {
	k.Tags[key] = value
	return k
}

func (k *KapacitorLoopbackNode) validate() error {
	if k.Database == "" {
		return errors.New("must specify a database")
	}
	if k.RetentionPolicy == "" {
		return errors.New("must specify a retention policy")
	}
	return nil
}
