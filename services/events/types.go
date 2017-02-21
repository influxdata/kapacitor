package events

// PubSub provides a publish subscribe API for internal Kapacitor events.
type PubSub interface {
	Publish(Event)
	Subscribe(Type) Subscription
}

// Types categorizes an event.
type Type string

type Event interface {
	Type() Type
}

type Subscription struct {
	Type Type
	C    <-chan Event
	c    chan Event
	s    *Service
}

func (s Subscription) Close() {
	s.s.unsubscribe(s)
}
