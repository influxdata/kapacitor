package notary

type Notary interface {
	Error(kv ...interface{}) error
	Debug(kv ...interface{}) error
	Info(kv ...interface{}) error
}

type Diagnostics interface {
	Diag(kv ...interface{})
}

//type Noter interface {
//	Note(kv ...Field)
//}
//
//type Field struct {
//}
