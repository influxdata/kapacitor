package events

import (
	"log"
	"sync"

	kexpvar "github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/vars"
)

const (
	eventBufferSize   = 5000
	statDroppedEvents = "dropped_events"
)

type Service struct {
	mu      sync.Mutex
	wg      sync.WaitGroup
	events  chan Event
	updates chan subUpdate
	closing chan struct{}
	opened  bool

	statsKey      string
	statMap       *kexpvar.Map
	droppedEvents *kexpvar.Int

	logger *log.Logger
}

func NewService(l *log.Logger) *Service {
	return &Service{
		logger: l,
	}
}

func (s *Service) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.opened {
		return nil
	}

	s.opened = true
	s.events = make(chan Event, eventBufferSize)
	s.updates = make(chan subUpdate)
	s.closing = make(chan struct{})

	s.statsKey, s.statMap = vars.NewStatistic("events", nil)
	s.droppedEvents = new(kexpvar.Int)
	s.statMap.Set(statDroppedEvents, s.droppedEvents)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.runEvents()
	}()
	return nil
}

func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.opened {
		return nil
	}
	vars.DeleteStatistic(s.statsKey)
	s.opened = false
	close(s.closing)
	s.wg.Wait()
	return nil
}

func (s *Service) Publish(e Event) {
	select {
	case <-s.closing:
	case s.events <- e:
	}
}

func (s *Service) Subscribe(t Type) Subscription {
	c := make(chan Event, eventBufferSize)
	sub := Subscription{
		C: c,
		c: c,
		s: s,
	}
	ua := subUpdate{
		Action:       addAction,
		Subscription: sub,
	}
	select {
	case <-s.closing:
	case s.updates <- ua:
	}
	return sub
}

func (s *Service) unsubscribe(sub Subscription) {
	ua := subUpdate{
		Action:       removeAction,
		Subscription: sub,
	}
	select {
	case <-s.closing:
	case s.updates <- ua:
	}
}

func (s *Service) runEvents() {
	subs := make(map[Type][]Subscription)
	for {
		select {
		case <-s.closing:
			return
		case u := <-s.updates:
			switch u.Action {
			case addAction:
				subs[u.Subscription.Type] = append(subs[u.Subscription.Type], u.Subscription)
			case removeAction:
				original := subs[u.Subscription.Type]
				filtered := original[0:0]
				for _, sub := range original {
					if sub.C != u.Subscription.C {
						filtered = append(filtered, sub)
					}
				}
				subs[u.Subscription.Type] = filtered
				close(u.Subscription.c)
			}
		case e := <-s.events:
			for _, sub := range subs[e.Type()] {
				select {
				case sub.c <- e:
				default:
					s.logger.Println("E! dropped event to subscription", e)
					s.droppedEvents.Add(1)
				}
			}
		}
	}
}

type updateAction int

const (
	addAction updateAction = iota
	removeAction
)

type subUpdate struct {
	Action       updateAction
	Subscription Subscription
}
