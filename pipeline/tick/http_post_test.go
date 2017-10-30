package tick_test

import (
	"testing"
	"time"
)

func TestHTTPPost(t *testing.T) {
	pipe, _, from := StreamFrom()
	post := from.HttpPost("http://influx1.local:8086/query", "http://influx2.local:8086/query")
	post.
		Endpoint("endpoint1").
		Endpoint("endpoint2").
		Header("Authorization", "Basic GOTO 10").
		Header("X-Forwarded-For", `10 PRINT "HELLO WORLD"`).
		CaptureResponse()
	post.CodeField = "statusField"
	post.Timeout = 10 * time.Second

	want := `stream
    |from()
    |httpPost('http://influx1.local:8086/query', 'http://influx2.local:8086/query')
        .codeField('statusField')
        .captureResponse()
        .timeout(10s)
        .endpoint('endpoint1')
        .endpoint('endpoint2')
        .header('Authorization', 'Basic GOTO 10')
        .header('X-Forwarded-For', '10 PRINT "HELLO WORLD"')
`
	PipelineTickTestHelper(t, pipe, want)
}
