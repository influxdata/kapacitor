package alert

import (
	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/models"
)

// HandlerSpecRegistrar is responsible for registering and persisting handler spec definitions.
type HandlerSpecRegistrar interface {
	// RegisterHandlerSpec saves the handler spec and registers a handler defined by the spec
	RegisterHandlerSpec(spec HandlerSpec) error
	// DeregisterHandlerSpec deletes the handler spec and deregisters the defined handler.
	DeregisterHandlerSpec(topic, id string) error
	// UpdateHandlerSpec updates the old spec with the new spec and takes care of registering new handlers based on the new spec.
	UpdateHandlerSpec(oldSpec, newSpec HandlerSpec) error
	// HandlerSpec returns a handler spec
	HandlerSpec(topic, id string) (HandlerSpec, bool, error)
	// Handlers returns a list of handler specs that match the pattern.
	HandlerSpecs(topic, pattern string) ([]HandlerSpec, error)
}

// Topics is responsible for querying the state of topics and their events.
type Topics interface {
	// TopicState returns the state of the specified topic,
	TopicState(topic string) (alert.TopicState, bool, error)
	// TopicStates returns the state of all topics that match the pattern and have at least minLevel.
	TopicStates(pattern string, minLevel alert.Level) (map[string]alert.TopicState, error)

	// EventState returns the current state of the event.
	EventState(topic, event string) (alert.EventState, bool, error)
	// EventStates returns the current state of events for the specified topic.
	// Only events greater or equal to minLevel will be returned
	EventStates(topic string, minLevel alert.Level) (map[string]alert.EventState, error)
}

// AnonHandlerRegistrar is responsible for directly registering handlers for anonymous topics.
// This is to be used only when the origin of the handler is not defined by a handler spec.
type AnonHandlerRegistrar interface {
	// RegisterHandler registers the handler instance for the listed topics.
	RegisterAnonHandler(topic string, h alert.Handler)
	// DeregisterHandler removes the handler from the listed topics.
	DeregisterAnonHandler(topic string, h alert.Handler)
}

type EventCollector interface {
	// Collect accepts a new event for processing.
	Collect(event alert.Event) error
}

// Events is responsible for accepting events for processing and reporting on the state of events.
type Events interface {
	EventCollector
	// UpdateEvent updates an existing event with a previously known state.
	UpdateEvent(topic string, event alert.EventState) error
	// EventState returns the current events state.
	EventState(topic, event string) (alert.EventState, bool, error)
}

// TopicPersister is responsible for controlling the persistence of topic state.
type TopicPersister interface {
	// CloseTopic closes a topic but does not delete its state.
	CloseTopic(topic string) error
	// DeleteTopic closes a topic and deletes all state associated with the topic.
	DeleteTopic(topic string) error
	// RestoreTopic signals that a topic should be restored from persisted state.
	RestoreTopic(topic string) error
}

type handler struct {
	Spec    HandlerSpec
	Handler alert.Handler
}

// InhibitorLookup provides lookup access to inhibitors
type InhibitorLookup interface {
	IsInhibited(name string, tags models.Tags) bool
	AddInhibitor(*alert.Inhibitor)
	RemoveInhibitor(*alert.Inhibitor)
}
