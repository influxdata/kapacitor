package tick_test

import (
	"testing"
	"time"

	"github.com/influxdata/kapacitor/pipeline"
)

func TestJoin(t *testing.T) {
	stream1 := &pipeline.StreamNode{}
	stream2 := &pipeline.StreamNode{}
	pipe := pipeline.CreatePipelineSources(stream1, stream2)

	from1 := stream1.From()
	from1.Measurement = "building_power"
	from1.GroupBy("building")

	from2 := stream2.From()
	from2.Measurement = "floor_power"
	from2.GroupBy("building", "floor")

	join := from1.Join(from2)
	join.As("building", "floor").On("building")
	join.Tolerance = time.Second
	join.StreamName = "kwh"

	got, err := PipelineTick(pipe)
	if err != nil {
		t.Fatalf("Unexpected error building pipeline %v", err)
	}
	want := `var from3 = stream
    |from()
        .measurement('floor_power')
        .groupBy('building', 'floor')

stream
    |from()
        .measurement('building_power')
        .groupBy('building')
    |join(from3)
        .as('building', 'floor')
        .on('building')
        .delimiter('.')
        .streamName('kwh')
        .tolerance(1s)
`
	if got != want {
		t.Errorf("TestJoin = %v, want %v", got, want)
		t.Log(got) // print is helpful to get the correct format.
	}
}
