package pipeline

import (
	"errors"
	"fmt"
	"strings"
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
	Endpoints []string `tick:"Endpoint"`

	// Headers
	Headers map[string]string `tick:"Header"`

	// tick:ignore
	URLs []string
}

func newHTTPPostNode(wants EdgeType, urls ...string) *HTTPPostNode {
	return &HTTPPostNode{
		chainnode: newBasicChainNode("http_post", wants, wants),
		URLs:      urls,
	}
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
