package tick_test

import (
	"testing"
	"time"
)

func TestQueryFlux(t *testing.T) {
	pipe, _, query := BatchQueryFlux(`from(bucket:"example-bucket")
|> range(start:-1h)
|> filter(fn:(r) =>
r._measurement == "cpu" and
r.cpu == "cpu-total"
)
|> aggregateWindow(every: 1m, fn: mean)`)

	query.Period = time.Minute
	query.Every = 30 * time.Second
	query.AlignFlag = true
	query.Offset = time.Hour
	query.Cluster = "mycluster"

	want := `batch
    |queryFlux('from(bucket:"example-bucket")
|> range(start:-1h)
|> filter(fn:(r) =>
r._measurement == "cpu" and
r.cpu == "cpu-total"
)
|> aggregateWindow(every: 1m, fn: mean)')
        .period(1m)
        .every(30s)
        .align()
        .offset(1h)
        .cluster('mycluster')
`
	PipelineTickTestHelper(t, pipe, want)
}
