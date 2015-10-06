package pipeline

// Caches the most recent item from the pipeline and exposes that item over an HTTP API.
type HTTPOutNode struct {
	node

	// The relative path where the cached data is exposed
	Endpoint string
}

func newHTTPOutNode(wants EdgeType, endpoint string) *HTTPOutNode {
	return &HTTPOutNode{
		node: node{
			desc:     "http_out",
			wants:    wants,
			provides: NoEdge,
		},
		Endpoint: endpoint,
	}
}
