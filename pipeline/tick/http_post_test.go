package tick_test

import (
	"testing"
	"time"
)

func TestHTTPPostURL(t *testing.T) {
	pipe, _, from := StreamFrom()
	post := from.HttpPost("http://influx1.local:8086/query")
	post.
		Header("Authorization", "Basic GOTO 10").
		Header("X-Forwarded-For", `10 PRINT "HELLO WORLD"`).
		CaptureResponse()
	post.CodeField = "statusField"
	post.Timeout = 10 * time.Second

	want := `stream
    |from()
    |httpPost('http://influx1.local:8086/query')
        .codeField('statusField')
        .captureResponse()
        .timeout(10s)
        .header('Authorization', 'Basic GOTO 10')
        .header('X-Forwarded-For', '10 PRINT "HELLO WORLD"')
`
	PipelineTickTestHelper(t, pipe, want)
}

func TestHTTPPostEndpoint(t *testing.T) {
	pipe, _, from := StreamFrom()
	post := from.HttpPost()
	post.
		Endpoint("endpoint1").
		Header("Authorization", "Basic GOTO 10").
		Header("X-Forwarded-For", `10 PRINT "HELLO WORLD"`).
		CaptureResponse()
	post.CodeField = "statusField"
	post.Timeout = 10 * time.Second

	want := `stream
    |from()
    |httpPost()
        .codeField('statusField')
        .captureResponse()
        .timeout(10s)
        .endpoint('endpoint1')
        .header('Authorization', 'Basic GOTO 10')
        .header('X-Forwarded-For', '10 PRINT "HELLO WORLD"')
`
	PipelineTickTestHelper(t, pipe, want)
}
