package diagnostic

type logger struct {
	enc Encoder
}

func NewLogger(enc Encoder) *logger {
	return &logger{enc: enc}
}

func (l *logger) Handle(keyvalsList ...[]interface{}) error {
	// Do whatever filtering I'd like
	// whether that be for the various log levels and the like
	// or update the type on encoder

	_ = l.enc.Encode(keyvalsList...)

	return nil
}
