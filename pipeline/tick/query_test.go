package tick_test

import (
	"testing"
	"time"
)

func TestQuery(t *testing.T) {
	pipe, _, query := BatchQuery("select cpu_usage from cpu")

	query.Period = time.Minute
	query.Every = 30 * time.Second
	query.AlignFlag = true
	query.Offset = time.Hour
	query.AlignGroupFlag = true
	query.Dimensions = []interface{}{"host", "region"}
	query.GroupByMeasurementFlag = true
	query.Fill = "linear"
	query.Cluster = "mycluster"

	want := `batch
    |query('select cpu_usage from cpu')
        .period(1m)
        .every(30s)
        .align()
        .offset(1h)
        .alignGroup()
        .groupBy(['host', 'region'])
        .groupByMeasurement()
        .fill('linear')
        .cluster('mycluster')
`
	PipelineTickTestHelper(t, pipe, want)
}
