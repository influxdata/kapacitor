package tick_test

import (
	"testing"
	"time"
)

func TestInfluxDBOut(t *testing.T) {
	pipe, _, from := StreamFrom()
	influx := from.InfluxDBOut()
	influx.Database = "mydb"
	influx.RetentionPolicy = "myrp"
	influx.Measurement = "errors"
	influx.Tag("kapacitor", "true")
	influx.Tag("version", "0.2")
	influx.WriteConsistency = "all"
	influx.Precision = "ms"
	influx.Buffer = 10
	influx.FlushInterval = time.Second
	influx.Create()

	want := `stream
    |from()
    |influxDBOut()
        .database('mydb')
        .retentionPolicy('myrp')
        .measurement('errors')
        .writeConsistency('all')
        .precision('ms')
        .buffer(10)
        .flushInterval(1s)
        .create()
        .tag('kapacitor', 'true')
        .tag('version', '0.2')
`
	PipelineTickTestHelper(t, pipe, want)
}
