package types

type Notary interface {
	//Note(kv ...interface{}) error
	Info(kv ...interface{}) error
	Error(kv ...interface{}) error
	Debug(kv ...interface{}) error
}
