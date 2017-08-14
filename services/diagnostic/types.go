package diagnostic

type Diagnosticer interface {
	Diag(...interface{}) error
}

type Service interface {
	NewDiagnosticer(Diagnosticer, ...interface{}) Diagnosticer
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
