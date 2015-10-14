package kapacitor

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"time"

	"github.com/influxdb/influxdb/influxql"
)

// The result from an output.
type Result influxql.Result

// Unmarshal a Result object from JSON.
func ResultFromJSON(in io.Reader) (r Result) {
	b, err := ioutil.ReadAll(in)
	if err != nil {
		r.Err = err
		return
	}

	json.Unmarshal(b, &r)
	// Convert all times to time.Time
	for _, series := range r.Series {
		for i, v := range series.Values {
			var t time.Time
			for j, c := range series.Columns {
				if c == "time" {
					tStr, ok := v[j].(string)
					if !ok {
						continue
					}
					t, err = time.Parse(time.RFC3339, tStr)
					if err != nil {
						continue
					}
					series.Values[i][j] = t
					break
				}
			}
		}
	}

	return
}
