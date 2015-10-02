package kapacitor

// An output of a pipeline. Still need to improve this interface to expose different types of outputs.
type Output interface {
	Endpoint() string
}
