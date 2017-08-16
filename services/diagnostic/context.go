package diagnostic

type context struct {
	keyvals []interface{}
	s       Subscriber // was previously *service
}

func (c *context) Diag(keyvals ...interface{}) error {
	// do validation of keys

	return c.s.Handle(c.keyvals, keyvals)
}
