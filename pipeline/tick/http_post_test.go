package tick_test

import (
	"fmt"
	"testing"
)

func TestHTTPPost(t *testing.T) {
	pipe, _, from := StreamFrom()
	post := from.HttpPost("http://influx1.local:8086/query", "http://influx2.local:8086/query")
	post.Endpoint("endpoint1").Endpoint("endpoint2").Header("Authorization", "Basic GOTO 10").Header("X-Forwarded-For", `10 PRINT "HELLO WORLD"`)
	got, err := PipelineTick(pipe)
	if err != nil {
		t.Fatalf("Unexpected error building pipeline %v", err)
	}

	want := `stream
    |from()
    |httpPost(['http://influx1.local:8086/query', 'http://influx2.local:8086/query'])
        .endpoint('endpoint1')
        .endpoint('endpoint2')
        .header('Authorization', 'Basic GOTO 10')
        .header('X-Forwarded-For', '10 PRINT "HELLO WORLD"')
`
	if got != want {
		t.Errorf("TestSample = %v, want %v", got, want)
		fmt.Println(got) // print is helpful to get the correct format.
	}
}
