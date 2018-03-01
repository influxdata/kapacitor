package pipeline

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/influxql"
)

const DefaultBufferSize = 1000
const DefaultFlushInterval = time.Second * 10

// Writes the data to InfluxDB as it is received.
//
// Example:
//    stream
//        |from()
//            .measurement('requests')
//        |eval(lambda: "errors" / "total")
//            .as('error_percent')
//        // Write the transformed data to InfluxDB
//        |influxDBOut()
//            .database('mydb')
//            .retentionPolicy('myrp')
//            .measurement('errors')
//            .tag('kapacitor', 'true')
//            .tag('version', '0.2')
//
// Available Statistics:
//
//    * points_written -- number of points written to InfluxDB
//    * write_errors -- number of errors attempting to write to InfluxDB
//
type InfluxDBOutNode struct {
	node `json:"-"`

	// The name of the InfluxDB instance to connect to.
	// If empty the configured default will be used.
	Cluster string `json:"cluster"`
	// The name of the database.
	Database string `json:"database"`
	// The name of the retention policy.
	RetentionPolicy string `json:"retentionPolicy"`
	// The name of the measurement.
	Measurement string `json:"measurement"`
	// The write consistency to use when writing the data.
	WriteConsistency string `json:"writeConsistency"`
	// The precision to use when writing the data.
	Precision string `json:"precision"`
	// Number of points to buffer when writing to InfluxDB.
	// Default: 1000
	Buffer int64 `json:"buffer"`
	// Write points to InfluxDB after interval even if buffer is not full.
	// Default: 10s
	FlushInterval time.Duration `json:"flushInterval"`
	// Static set of tags to add to all data points before writing them.
	// tick:ignore
	Tags map[string]string `tick:"Tag" json:"tags"`
	// Create the specified database and retention policy
	// tick:ignore
	CreateFlag bool `tick:"Create" json:"create"`
}

func newInfluxDBOutNode(wants EdgeType) *InfluxDBOutNode {
	return &InfluxDBOutNode{
		node: node{
			desc:     "influxdb_out",
			wants:    wants,
			provides: NoEdge,
		},
		Tags:          make(map[string]string),
		Buffer:        DefaultBufferSize,
		FlushInterval: DefaultFlushInterval,
	}
}

// MarshalJSON converts InfluxDBOutNode to JSON
// tick:ignore
func (n *InfluxDBOutNode) MarshalJSON() ([]byte, error) {
	type Alias InfluxDBOutNode
	var raw = &struct {
		TypeOf
		*Alias
		FlushInterval string `json:"flushInterval"`
	}{
		TypeOf: TypeOf{
			Type: "influxdbOut",
			ID:   n.ID(),
		},
		Alias:         (*Alias)(n),
		FlushInterval: influxql.FormatDuration(n.FlushInterval),
	}
	return json.Marshal(raw)
}

// UnmarshalJSON converts JSON to an InfluxDBOutNode
// tick:ignore
func (n *InfluxDBOutNode) UnmarshalJSON(data []byte) error {
	type Alias InfluxDBOutNode
	var raw = &struct {
		TypeOf
		*Alias
		FlushInterval string `json:"flushInterval"`
	}{
		Alias: (*Alias)(n),
	}
	err := json.Unmarshal(data, raw)
	if err != nil {
		return err
	}
	if raw.Type != "influxdbOut" {
		return fmt.Errorf("error unmarshaling node %d of type %s as InfluxDBOutNode", raw.ID, raw.Type)
	}
	n.FlushInterval, err = influxql.ParseDuration(raw.FlushInterval)
	if err != nil {
		return err
	}
	n.setID(raw.ID)
	return nil
}

// Add a static tag to all data points.
// Tag can be called more then once.
//
// tick:property
func (i *InfluxDBOutNode) Tag(key, value string) *InfluxDBOutNode {
	i.Tags[key] = value
	return i
}

// Create indicates that both the database and retention policy
// will be created, when the task is started.
// If the retention policy name is empty then no
// retention policy will be specified and
// the default retention policy name will be created.
//
// If the database already exists nothing happens.
//
// tick:property
func (i *InfluxDBOutNode) Create() *InfluxDBOutNode {
	i.CreateFlag = true
	return i
}
