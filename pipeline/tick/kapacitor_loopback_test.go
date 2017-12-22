package tick_test

import (
	"testing"
)

func TestKapacitorLoopback(t *testing.T) {
	pipe, _, from := StreamFrom()
	loop := from.KapacitorLoopback()
	loop.Database = "mydb"
	loop.RetentionPolicy = "myrp"
	loop.Measurement = "meas"
	loop.Tag("vocabulary", "volcano")
	loop.Tag("season", "winter")

	want := `stream
    |from()
    |kapacitorLoopback()
        .database('mydb')
        .retentionPolicy('myrp')
        .measurement('meas')
        .tag('season', 'winter')
        .tag('vocabulary', 'volcano')
`
	PipelineTickTestHelper(t, pipe, want)
}
