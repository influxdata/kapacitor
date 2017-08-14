package kapacitor

// TODO: idk about this

type Notary interface {
	Error(kv ...interface{}) error
	Debug(kv ...interface{}) error
	Info(kv ...interface{}) error
}
