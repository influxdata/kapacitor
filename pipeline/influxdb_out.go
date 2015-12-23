package pipeline

// Writes the data to InfluxDB as it is received.
//
// Example:
//    stream
//        .eval(lambda: "errors" / "total")
//            .as('error_percent')
//        // Write the transformed data to InfluxDB
//        .influxDBOut()
//            .database('mydb')
//            .retentionPolicy('myrp')
//            .measurement('errors')
//            .tag('kapacitor', 'true')
//            .tag('version', '0.2')
//
type InfluxDBOutNode struct {
	node

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
	// Static set of tags to add to all data points before writing them.
	//tick:ignore
	Tags map[string]string
}

func newInfluxDBOutNode(wants EdgeType) *InfluxDBOutNode {
	return &InfluxDBOutNode{
		node: node{
			desc:     "influxdb_out",
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
func (i *InfluxDBOutNode) Tag(key, value string) *InfluxDBOutNode {
	i.Tags[key] = value
	return i
}
