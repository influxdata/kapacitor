package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/influxdata/influxdb/influxql"
)

// An HTTPPostNode will take the incoming data stream and POST it to an HTTP endpoint.
// That endpoint may be specified as a positional argument, or as an endpoint property
// method on httpPost. Multiple endpoint property methods may be specified.
//
// Example:
//    stream
//        |window()
//            .period(10s)
//            .every(5s)
//        |top('value', 10)
//        //Post the top 10 results over the last 10s updated every 5s.
//        |httpPost('http://example.com/api/top10')
//
// Example:
//    stream
//        |window()
//            .period(10s)
//            .every(5s)
//        |top('value', 10)
//        //Post the top 10 results over the last 10s updated every 5s.
//        |httpPost()
//            .endpoint('example')
//
type HTTPPostNode struct {
	chainnode

	// tick:ignore
	Endpoints []string `tick:"Endpoint" json:"endpoints"`

	// Headers
	// tick:ignore
	Headers map[string]string `tick:"Header" json:"headers"`

	// CodeField is the name of the field in which to place the HTTP status code.
	// If the HTTP request fails at a layer below HTTP, (i.e. rejected TCP connection), then the status code is set to 0.
	CodeField string `json:"codeField"`

	// tick:ignore
	CaptureResponseFlag bool `tick:"CaptureResponse" json:"captureResponse"`

	// tick:ignore
	URLs []string `json:"urls"`

	// Timeout for HTTP Post
	Timeout time.Duration `json:"timeout"`
}

func newHTTPPostNode(wants EdgeType, urls ...string) *HTTPPostNode {
	return &HTTPPostNode{
		chainnode: newBasicChainNode("http_post", wants, wants),
		URLs:      urls,
	}
}

// MarshalJSON converts HTTPPostNode to JSON
// tick:ignore
func (n *HTTPPostNode) MarshalJSON() ([]byte, error) {
	type Alias HTTPPostNode
	var raw = &struct {
		TypeOf
		*Alias
		Timeout string `json:"timeout"`
	}{
		TypeOf: TypeOf{
			Type: "httpPost",
			ID:   n.ID(),
		},
		Alias:   (*Alias)(n),
		Timeout: influxql.FormatDuration(n.Timeout),
	}
	return json.Marshal(raw)
}

// UnmarshalJSON converts JSON to an HTTPPostNode
// tick:ignore
func (n *HTTPPostNode) UnmarshalJSON(data []byte) error {
	type Alias HTTPPostNode
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
	if raw.Type != "httpPost" {
		return fmt.Errorf("error unmarshaling node %d of type %s as HTTPPostNode", raw.ID, raw.Type)
	}
	n.setID(raw.ID)
	return nil
}

// tick:ignore
func (p *HTTPPostNode) validate() error {
	if len(p.URLs) >= 2 {
		return fmt.Errorf("httpPost expects 0 or 1 arguments, got %v", len(p.URLs))
	}

	if len(p.Endpoints) > 1 {
		return fmt.Errorf("httpPost expects 0 or 1 endpoints, got %v", len(p.Endpoints))
	}

	if len(p.URLs) == 0 && len(p.Endpoints) == 0 {
		return errors.New("must provide url or endpoint")
	}

	if len(p.URLs) > 0 && len(p.Endpoints) > 0 {
		return errors.New("only one endpoint and url may be specified")
	}

	for k := range p.Headers {
		if strings.ToUpper(k) == "AUTHENTICATE" {
			return errors.New("cannot set 'authenticate' header")
		}
	}

	return nil
}

// Name of the endpoint to be used, as is defined in the configuration file.
//
// Example:
//    stream
//         |httpPost()
//            .endpoint('example')
//
// tick:property
func (p *HTTPPostNode) Endpoint(endpoint string) *HTTPPostNode {
	p.Endpoints = append(p.Endpoints, endpoint)
	return p
}

// Add a header to the POST request
//
// Example:
//    stream
//         |httpPost()
//            .endpoint('example')
//              .header('my', 'header')
//
// tick:property
func (p *HTTPPostNode) Header(k, v string) *HTTPPostNode {
	if p.Headers == nil {
		p.Headers = map[string]string{}
	}
	p.Headers[k] = v

	return p
}

// CaptureResponse indicates that the HTTP response should be read and logged if
// the status code was not an 2xx code.
// tick:property
func (p *HTTPPostNode) CaptureResponse() *HTTPPostNode {
	p.CaptureResponseFlag = true
	return p
}
