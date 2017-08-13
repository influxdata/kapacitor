package notary

import "time"

type Notary interface {
	//Note(kv ...interface{}) error
	Info(kv ...interface{}) error
	Error(kv ...interface{}) error
	Debug(kv ...interface{}) error
}

type Noter interface {
	Log(kv ...Field)
}

type Field struct {
}

func WithPrefix(logger Notary, kv ...interface{}) *context {
	n.Note(
		String("level", "crit"),
		Int("attempt", 3),
		Duration("backoff", time.Second),
	)

	return nil
}

type context struct {
}

func (t *context) Info(kv ...interface{}) error {
	return nil
}
func (t *context) Note(kv ...interface{}) error {
	return nil
}
func (t *context) Debug(kv ...interface{}) error {
	return nil
}
func (t *context) Error(kv ...interface{}) error {
	return nil
}
func (t *context) Other(kv ...interface{}) error {
	return nil
}

func New(prefixs ...interface{}) *context {
	return &context{}
}
