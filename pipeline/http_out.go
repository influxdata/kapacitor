package pipeline

import (
	"encoding/json"
	"fmt"
)

// An HTTPOutNode caches the most recent data for each group it has received.
//
// The cached data is available at the given endpoint.
// The endpoint is the relative path from the API endpoint of the running task.
// For example if the task endpoint is at `/kapacitor/v1/tasks/<task_id>` and endpoint is
// `top10`, then the data can be requested from `/kapacitor/v1/tasks/<task_id>/top10`.
//
// Example:
//    stream
//        |window()
//            .period(10s)
//            .every(5s)
//        |top('value', 10)
//        //Publish the top 10 results over the last 10s updated every 5s.
//        |httpOut('top10')
//
// Beware of adding a final slash ‘/’ to the URL. This will result in a 404 error for a
// task that does not exist.
//
// Note that the example script above comes from the
// [scores](https://github.com/influxdata/kapacitor/tree/master/examples/scores) example.
// See the complete scores example for a concrete demonstration.
//
type HTTPOutNode struct {
	chainnode

	// The relative path where the cached data is exposed
	// tick:ignore
	Endpoint string `json:"endpoint"`
}

func newHTTPOutNode(wants EdgeType, endpoint string) *HTTPOutNode {
	return &HTTPOutNode{
		chainnode: newBasicChainNode("http_out", wants, wants),
		Endpoint:  endpoint,
	}
}

// MarshalJSON converts HTTPOutNode to JSON
// tick:ignore
func (n *HTTPOutNode) MarshalJSON() ([]byte, error) {
	type Alias HTTPOutNode
	var raw = &struct {
		TypeOf
		*Alias
	}{
		TypeOf: TypeOf{
			Type: "httpOut",
			ID:   n.ID(),
		},
		Alias: (*Alias)(n),
	}
	return json.Marshal(raw)
}

// UnmarshalJSON converts JSON to an HTTPOutNode
// tick:ignore
func (n *HTTPOutNode) UnmarshalJSON(data []byte) error {
	type Alias HTTPOutNode
	var raw = &struct {
		TypeOf
		*Alias
	}{
		Alias: (*Alias)(n),
	}
	err := json.Unmarshal(data, raw)
	if err != nil {
		return err
	}
	if raw.Type != "httpOut" {
		return fmt.Errorf("error unmarshaling node %d of type %s as HTTPOutNode", raw.ID, raw.Type)
	}
	n.setID(raw.ID)
	return nil
}
