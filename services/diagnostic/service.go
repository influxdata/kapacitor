package diagnostic

import "sync"

type service struct {
	subMu       sync.RWMutex
	subscribers []Subscriber
}

func NewService() Service {
	s := &service{}

	return s
}

func (s *service) Open() error {
	return nil
}

func (s *service) Close() error {
	return nil
}

func (s *service) SubscribeAll(sub Subscriber) error {
	s.subMu.Lock()
	defer s.subMu.Unlock()

	s.subscribers = append(s.subscribers, sub)

	return nil
}

func (s *service) Handle(keyvalList ...[]interface{}) error {
	s.subMu.RLock()
	defer s.subMu.RUnlock()

	for _, subscriber := range s.subscribers {
		// handle error
		_ = subscriber.Handle(keyvalList...)
	}

	return nil
}

func (s *service) NewDiagnostic(d Diagnostic, keyvals ...interface{}) Diagnostic {
	ctx := &context{
		s: s,
	}

	if c, ok := d.(*context); ok {
		// TODO: make sure this is right
		ctx.keyvals = make([]interface{}, len(c.keyvals))
		copy(ctx.keyvals, c.keyvals)
	}

	ctx.keyvals = append(ctx.keyvals, keyvals...)

	return ctx
}
