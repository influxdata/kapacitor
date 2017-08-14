package notary

func WithPrefix(logger Notary, kv ...interface{}) *context {

	return nil
}

type context struct {
	fields []interface{}
	n      Notary
}

func (t *context) Info(kv ...interface{}) error {
	return t.n.Info(kv...)
}
func (t *context) Debug(kv ...interface{}) error {
	return nil
}
func (t *context) Error(kv ...interface{}) error {
	return t.n.Error(kv...)
}
func (t *context) Other(kv ...interface{}) error {
	return nil
}

func New(prefixs ...interface{}) *context {
	return &context{}
}

func WithContext(n Notary, prefixs ...interface{}) *context {
	return &context{
		n: n,
	}
}
