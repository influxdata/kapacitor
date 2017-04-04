package pipeline

// An HTTPPostNode will take the incoming data stream and POST it to an HTTP endpoint.
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
type HTTPPostNode struct {
	chainnode

	// tick:ignore
	Url string
}

func newHTTPPostNode(wants EdgeType, url string) *HTTPPostNode {
	return &HTTPPostNode{
		chainnode: newBasicChainNode("http_post", wants, wants),
		Url:       url,
	}
}
