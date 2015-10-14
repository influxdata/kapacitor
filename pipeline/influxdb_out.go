package pipeline

// Writes the data to InfluxDB
type InfluxDBOutNode struct {
	node

	Database         string
	RetentionPolicy  string
	Measurement      string
	WriteConsistency string
	Precision        string
	Tags             map[string]string
}

func newInfluxDBOutNode(wants EdgeType) *InfluxDBOutNode {
	return &InfluxDBOutNode{
		node: node{
			desc:     "influxdb_out",
			wants:    wants,
			provides: NoEdge,
		},
	}
}
