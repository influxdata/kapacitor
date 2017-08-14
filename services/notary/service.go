package notary

import (
	"os"
)

func WithPrefix(logger Notary, kv ...interface{}) *context {

	// TODO: fix
	return WithContext(NewPairLogger(os.Stdout), kv...)
}

type context struct {
	fields []interface{}
	n      Notary
}

func (t *context) Info(kv ...interface{}) error {
	if t == nil {
		return nil
	}
	return t.n.Info(kv...)
}
func (t *context) Debug(kv ...interface{}) error {
	return nil
}
func (t *context) Error(kv ...interface{}) error {
	return t.n.Info(kv...)
}
func (t *context) Other(kv ...interface{}) error {
	return nil
}

func New(prefixs ...interface{}) *context {
	return WithContext(NewPairLogger(os.Stdout), prefixs...)
}

func WithContext(n Notary, prefixs ...interface{}) *context {
	return &context{
		n: n,
	}
}
