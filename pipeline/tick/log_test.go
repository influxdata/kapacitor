package tick_test

import (
	"testing"
)

func TestLog(t *testing.T) {
	pipe, _, query := BatchQuery("select cpu_usage from cpu")
	logger := query.Log()
	logger.Level = "ERROR"
	logger.Prefix = "oh no"
	logger.Log() // default options

	want := `batch
    |query('select cpu_usage from cpu')
    |log()
        .level('ERROR')
        .prefix('oh no')
    |log()
        .level('INFO')
`
	PipelineTickTestHelper(t, pipe, want)
}
