package pipeline

import "time"

const DefaultBufferSize = 1000
const DefaultFlushInterval = time.Second * 10

// Writes the data to InfluxDB as it is received.
//
// Example:
//    stream
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
	node

	// The name of the InfluxDB instance to connect to.
	// If empty the configured default will be used.
	Cluster string
	// The name of the database.
	Database string
	// The name of the retention policy.
	RetentionPolicy string
	// The name of the measurement.
	Measurement string
	// The write consistency to use when writing the data.
	WriteConsistency string
	// The precision to use when writing the data.
	Precision string
	// Number of points to buffer when writing to InfluxDB.
	// Default: 1000
	Buffer int64
	// Write points to InfluxDB after interval even if buffer is not full.
	// Default: 10s
	FlushInterval time.Duration
	// Static set of tags to add to all data points before writing them.
	// tick:ignore
	Tags map[string]string `tick:"Tag"`
	// Create the specified database and retention policy
	// tick:ignore
	CreateFlag bool `tick:"Create"`
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

// Add a static tag to all data points.
// Tag can be called more than once.
//
// tick:property
func (i *InfluxDBOutNode) Tag(key, value string) *InfluxDBOutNode {
	i.Tags[key] = value
	return i
}

// Create indicates that both the database and retention policy
// will be created, when the task is started.
// If the retention policy name is empty than no
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
