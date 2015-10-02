package kapacitor

import (
	"encoding/json"
	"io"
	"io/ioutil"

	"github.com/influxdb/kapacitor/models"
)

// The result from an output.
type Result struct {
	Err    error
	Window map[models.GroupID][]*models.Point
}

// Unmarshal a Result object from JSON.
func ResultFromJSON(in io.Reader) (r *Result) {
	r = &Result{}
	b, err := ioutil.ReadAll(in)
	if err != nil {
		r.Err = err
		return
	}

	json.Unmarshal(b, &r.Window)

	return
}
