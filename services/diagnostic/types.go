package diagnostic

type Diagnostic interface {
	Diag(...interface{}) error
}

type Service interface {
	NewDiagnostic(Diagnostic, ...interface{}) Diagnostic
	//Subscribe(key string, subscr Subscriber) error
	SubscribeAll(subscr Subscriber) error
}

type Subscriber interface {
	// maybe modify interface to be
	//Handle([][]interface{}) error
	// if it will avoid additional alocation

	Handle(...[]interface{}) error
}

type Encoder interface {
	Encode(...[]interface{}) error
}
