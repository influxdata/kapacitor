package kapacitor

// TODO: idk about this

type Notary interface {
	Diag(msg string, kv ...interface{})
}
