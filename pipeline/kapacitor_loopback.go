package pipeline

import (
	"errors"
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
	node

	// The name of the database.
	Database string
	// The name of the retention policy.
	RetentionPolicy string
	// The name of the measurement.
	Measurement string
	// Static set of tags to add to all data points before writing them.
	// tick:ignore
	Tags map[string]string `tick:"Tag"`
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
