package alert

import (
	"bytes"
	"encoding"
	"encoding/json"
	"fmt"
	"path"
	"reflect"
	"regexp"
	"sync"

	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/command"
	"github.com/influxdata/kapacitor/keyvalue"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/services/alerta"
	"github.com/influxdata/kapacitor/services/bigpanda"
	"github.com/influxdata/kapacitor/services/discord"
	"github.com/influxdata/kapacitor/services/hipchat"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/influxdata/kapacitor/services/httppost"
	"github.com/influxdata/kapacitor/services/kafka"
	"github.com/influxdata/kapacitor/services/mqtt"
	"github.com/influxdata/kapacitor/services/opsgenie"
	"github.com/influxdata/kapacitor/services/opsgenie2"
	"github.com/influxdata/kapacitor/services/pagerduty"
	"github.com/influxdata/kapacitor/services/pagerduty2"
	"github.com/influxdata/kapacitor/services/pushover"
	"github.com/influxdata/kapacitor/services/sensu"
	"github.com/influxdata/kapacitor/services/servicenow"
	"github.com/influxdata/kapacitor/services/slack"
	"github.com/influxdata/kapacitor/services/smtp"
	"github.com/influxdata/kapacitor/services/snmptrap"
	"github.com/influxdata/kapacitor/services/storage"
	"github.com/influxdata/kapacitor/services/teams"
	"github.com/influxdata/kapacitor/services/telegram"
	"github.com/influxdata/kapacitor/services/victorops"
	"github.com/influxdata/kapacitor/services/zenoss"
	"github.com/mailru/easyjson/jlexer"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
)

type Diagnostic interface {
	WithHandlerContext(ctx ...keyvalue.T) HandlerDiagnostic

	MigratingHandlerSpecs()
	FoundHandlerRows(length int)
	FoundNewHandler(key string)
	CreatingNewHandlers(length int)
	MigratingOldHandlerSpec(id string)

	Error(msg string, err error, ctx ...keyvalue.T)
	Info(msg string, ctx ...keyvalue.T)
}

type StorageService interface {
	Store(namespace string) storage.Interface
	Register(name string, store storage.StoreActioner)
	Versions() storage.Versions
	Diagnostic() storage.Diagnostic
	Path() string
	CloseBolt() error
}

type Service struct {
	mu       sync.RWMutex
	disabled map[string]struct{}
	// Handler store API
	specsDAO HandlerSpecDAO
	// V2 topic store
	topicsStore   storage.Interface
	PersistTopics bool

	APIServer *apiServer

	handlers map[string]map[string]handler

	closedTopics map[string]bool

	inhibitorLookup *alert.InhibitorLookup

	topics         *alert.Topics
	EventCollector EventCollector

	HTTPDService interface {
		AddRoutes([]httpd.Route) error
		DelRoutes([]httpd.Route)
	}

	StorageService StorageService

	Commander command.Commander

	diag Diagnostic

	AlertaService interface {
		DefaultHandlerConfig() alerta.HandlerConfig
		Handler(alerta.HandlerConfig, ...keyvalue.T) (alert.Handler, error)
	}
	BigPandaService interface {
		Handler(bigpanda.HandlerConfig, ...keyvalue.T) (alert.Handler, error)
	}
	HipChatService interface {
		Handler(hipchat.HandlerConfig, ...keyvalue.T) alert.Handler
	}
	KafkaService interface {
		Handler(kafka.HandlerConfig, ...keyvalue.T) (alert.Handler, error)
	}
	MQTTService interface {
		Handler(mqtt.HandlerConfig, ...keyvalue.T) (alert.Handler, error)
	}
	OpsGenieService interface {
		Handler(opsgenie.HandlerConfig, ...keyvalue.T) alert.Handler
	}
	OpsGenie2Service interface {
		Handler(opsgenie2.HandlerConfig, ...keyvalue.T) alert.Handler
	}
	PagerDutyService interface {
		Handler(pagerduty.HandlerConfig, ...keyvalue.T) alert.Handler
	}
	PagerDuty2Service interface {
		Handler(pagerduty2.HandlerConfig, ...keyvalue.T) (alert.Handler, error)
	}
	PushoverService interface {
		Handler(pushover.HandlerConfig, ...keyvalue.T) alert.Handler
	}
	HTTPPostService interface {
		Handler(httppost.HandlerConfig, ...keyvalue.T) (alert.Handler, error)
	}
	SensuService interface {
		Handler(sensu.HandlerConfig, ...keyvalue.T) (alert.Handler, error)
	}
	SlackService interface {
		Handler(slack.HandlerConfig, ...keyvalue.T) alert.Handler
	}
	DiscordService interface {
		Handler(discord.HandlerConfig, ...keyvalue.T) (alert.Handler, error)
	}
	SMTPService interface {
		Handler(smtp.HandlerConfig, ...keyvalue.T) alert.Handler
	}
	SNMPTrapService interface {
		Handler(snmptrap.HandlerConfig, ...keyvalue.T) (alert.Handler, error)
	}
	TalkService interface {
		Handler(...keyvalue.T) alert.Handler
	}
	TelegramService interface {
		Handler(telegram.HandlerConfig, ...keyvalue.T) alert.Handler
	}
	VictorOpsService interface {
		Handler(victorops.HandlerConfig, ...keyvalue.T) alert.Handler
	}
	TeamsService interface {
		Handler(teams.HandlerConfig, ...keyvalue.T) alert.Handler
	}
	ServiceNowService interface {
		Handler(servicenow.HandlerConfig, ...keyvalue.T) alert.Handler
	}
	ZenossService interface {
		Handler(zenoss.HandlerConfig, ...keyvalue.T) alert.Handler
	}
}

func NewService(d Diagnostic, disabled map[string]struct{}, topicBufLen int) *Service {
	s := &Service{
		disabled:        disabled,
		handlers:        make(map[string]map[string]handler),
		closedTopics:    make(map[string]bool),
		topics:          alert.NewTopics(topicBufLen),
		diag:            d,
		inhibitorLookup: alert.NewInhibitorLookup(),
	}
	s.APIServer = &apiServer{
		Registrar: s,
		Topics:    s,
		Persister: s,
		diag:      d,
	}
	s.EventCollector = s
	return s
}

const (
	// Public name of the handler specs store.
	handlerSpecsAPIName = "handler-specs"
	// The storage namespace V1 topic store and task data.
	// In V2, still stores handlers
	AlertNameSpace = "alert_store"
	// TopicStatesNameSpace - The storage namespace for the V2 topic store and nothing else
	TopicStatesNameSpace = "topic_states_store"
)

func (s *Service) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Create DAO
	store := s.StorageService.Store(AlertNameSpace)
	specsDAO, err := newHandlerSpecKV(store)
	if err != nil {
		return err
	}
	s.specsDAO = specsDAO
	s.StorageService.Register(handlerSpecsAPIName, s.specsDAO)
	s.topicsStore = s.StorageService.Store(TopicStatesNameSpace)
	// NOTE: since the topics store doesn't use the indexing store, we don't need to register the api

	// Migrate v1.2 handlers
	if err := s.migrateHandlerSpecs(store); err != nil {
		return err
	}

	// Load saved handlers
	if err := s.loadSavedHandlerSpecs(); err != nil {
		return err
	}

	if err := s.MigrateTopicStoreV1V2(); err != nil {
		return err
	}

	// Load saved topic state
	if err := s.loadSavedTopicStates(); err != nil {
		return err
	}

	s.APIServer.HTTPDService = s.HTTPDService
	if err := s.APIServer.Open(); err != nil {
		return err
	}

	return nil
}

func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.topics.Close()
	return s.APIServer.Close()
}

const (
	handlerSpecsStoreVersion  = "alert_topic_handler_specs"
	handlerSpecsStoreVersion1 = "1"
)

func (s *Service) migrateHandlerSpecs(store storage.Interface) error {
	specVersion, err := s.StorageService.Versions().Get(handlerSpecsStoreVersion)
	if err != nil && err != storage.ErrNoKeyExists {
		return err
	}
	if specVersion == handlerSpecsStoreVersion1 {
		// Already migrated
		return nil
	}
	s.diag.MigratingHandlerSpecs()

	// v1.2 HandlerActionSpec
	type oldHandlerActionSpec struct {
		Kind    string                 `json:"kind"`
		Options map[string]interface{} `json:"options"`
	}

	// v1.2 HandlerSpec
	type oldHandlerSpec struct {
		ID      string                 `json:"id"`
		Topics  []string               `json:"topics"`
		Actions []oldHandlerActionSpec `json:"actions"`
	}
	oldDataPrefix := "/" + handlerPrefix + "/data"
	oldKeyPattern := regexp.MustCompile(fmt.Sprintf(`^%s/[-\._\p{L}0-9]+$`, oldDataPrefix))

	// Process to migrate to new handler specs:
	//     1. Gather all old handlers
	//     2. Define new handlers that are equivalent
	//     3. Check that there are no ID conflicts
	//     4. Save the new handlers
	//     5. Delete the old specs
	//
	// All steps are performed in a single transaction,
	// so it can be rolledback in case of an error.
	err = store.Update(func(tx storage.Tx) error {
		var newHandlers []HandlerSpec
		kvs, err := tx.List(oldDataPrefix)
		if err != nil {
			return err
		}
		s.diag.FoundHandlerRows(len(kvs))

		for _, kv := range kvs {
			if !oldKeyPattern.MatchString(kv.Key) {
				s.diag.FoundNewHandler(kv.Key)
				continue
			}
			var old oldHandlerSpec
			err := storage.VersionJSONDecode(kv.Value, func(version int, dec *json.Decoder) error {
				if version != 1 {
					return fmt.Errorf("old handler specs should all be version 1, got version %d", version)
				}
				return dec.Decode(&old)
			})
			if err != nil {
				return errors.Wrapf(err, "failed to read old handler spec data for %s", kv.Key)
			}

			s.diag.MigratingOldHandlerSpec(old.ID)

			// Create new handlers from the old
			hasStateChangesOnly := false
			aggregatePrefix := ""
			for i, action := range old.Actions {
				new := HandlerSpec{
					ID:      fmt.Sprintf("%s-%s-%d", old.ID, action.Kind, i),
					Kind:    action.Kind,
					Options: action.Options,
				}
				if hasStateChangesOnly {
					new.Match = "changed() == TRUE"
				}
				switch action.Kind {
				case "stateChangesOnly":
					hasStateChangesOnly = true
					// No need to add a handler for this one
				case "aggregate":
					newPrefix := aggregatePrefix + "aggregate_topic-" + old.ID + "-"
					for _, topic := range old.Topics {
						new.Topic = aggregatePrefix + topic
						new.Options["topic"] = newPrefix + topic
						newHandlers = append(newHandlers, new)
					}
					aggregatePrefix = newPrefix
				default:
					for _, topic := range old.Topics {
						new.Topic = aggregatePrefix + topic
						newHandlers = append(newHandlers, new)
					}
				}
			}
		}

		// Check that all new handlers are unique
		for _, handler := range newHandlers {
			if _, err := s.specsDAO.GetTx(tx, handler.Topic, handler.ID); err != ErrNoHandlerSpecExists {
				return fmt.Errorf("handler %q for topic %q already exists", handler.ID, handler.Topic)
			}
		}

		s.diag.CreatingNewHandlers(len(newHandlers))

		// Create new handlers
		for _, handler := range newHandlers {
			if err := s.specsDAO.CreateTx(tx, handler); err != nil {
				return errors.Wrap(err, "failed to create new handler during migration")
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	// Save version
	return s.StorageService.Versions().Set(handlerSpecsStoreVersion, handlerSpecsStoreVersion1)
}

func (s *Service) loadSavedHandlerSpecs() error {
	offset := 0
	limit := 100
	for {
		specs, err := s.specsDAO.List("*", "", offset, limit)
		if err != nil {
			return err
		}

		for _, spec := range specs {
			if err := s.loadHandlerSpec(spec); err != nil {
				s.diag.Error("failed to load handler on startup", err)
			}
		}

		offset += limit
		if len(specs) != limit {
			break
		}
	}
	return nil
}

func convertEventStateToAlert(id string, state *EventState) *alert.EventState {
	return &alert.EventState{
		ID:       id,
		Message:  state.Message,
		Details:  state.Details,
		Time:     state.Time,
		Duration: state.Duration,
		Level:    state.Level,
	}
}

func convertEventStateFromAlert(state alert.EventState) *EventState {
	return &EventState{
		Message:  state.Message,
		Details:  state.Details,
		Time:     state.Time,
		Duration: state.Duration,
		Level:    state.Level,
	}
}

func (s *Service) loadSavedTopicStates() error {
	buf := bytes.Buffer{}
	return WalkTopicBuckets(s.topicsStore, func(tx storage.ReadOnlyTx, topic string) error {
		_, _ = buf.WriteString(topic) // WriteString error is always nil
		eventStates, err := s.loadConvertTopicBucket(tx, buf.Bytes())
		if err != nil {
			return err
		}
		s.topics.RestoreTopicNoCopy(topic, eventStates)
		buf.Reset()
		return nil
	})
}

func WalkTopicBuckets(topicsStore storage.Interface, fn func(tx storage.ReadOnlyTx, topic string) error) error {
	return topicsStore.View(func(tx storage.ReadOnlyTx) error {
		kv, err := tx.List("")
		if err != nil {
			return fmt.Errorf("cannot retrieve topic list: %w", err)
		}

		for _, b := range kv {
			if b == nil {
				continue
			}
			if err = fn(tx, b.Key); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Service) loadConvertTopicBucket(tx storage.ReadOnlyTx, topic []byte) (map[string]*alert.EventState, error) {
	q, err := tx.Bucket(topic).List("")
	if err != nil {
		return nil, err
	}
	eventstates := make(map[string]*alert.EventState, len(q))
	es := &EventState{} //create a buffer to hold the unmarshalled EventState
	for _, b := range q {
		err = es.UnmarshalJSON(b.Value)
		if err != nil {
			return nil, err
		}
		eventstates[b.Key] = convertEventStateToAlert(b.Key, es)
		es.Reset()
	}
	return eventstates, nil
}

func validatePattern(pattern string) error {
	_, err := path.Match(pattern, "")
	return err
}

// set a topic handler in the internal map, caller must have lock.
func (s *Service) setTopicHandler(topic, id string, h handler) {
	if s.handlers[topic] == nil {
		s.handlers[topic] = make(map[string]handler)
	}
	s.handlers[topic][id] = h
}

func (s *Service) Collect(event alert.Event) error {
	s.mu.RLock()
	closed := s.closedTopics[event.Topic]
	s.mu.RUnlock()
	if closed {
		// Restore topic
		if err := s.restoreClosedTopic(event.Topic); err != nil {
			return err
		}
	}

	err := s.topics.Collect(event)
	if err != nil {
		return err
	}
	// Events with alert.OK status should always only be resets from other statuses.
	if event.State.Level == alert.OK && s.PersistTopics {
		if err := s.clearHistory(&event); err != nil {
			return fmt.Errorf("failed to clear event history for topic %q: %w", event.Topic, err)
		} else {
			return nil
		}
	} else {
		return s.persistEventState(event)
	}
}

func (s *Service) persistEventState(event alert.Event) error {
	if !s.PersistTopics {
		return nil
	}

	if _, ok := s.topics.Topic(event.Topic); !ok {
		// Topic was deleted since event was collected, nothing to do.
		return nil
	}

	return s.topicsStore.Update(func(tx storage.Tx) error {
		tx = tx.Bucket([]byte(event.Topic))
		if tx == nil {
			return nil
		}
		data, err := convertEventStateFromAlert(event.State).MarshalJSON()
		if err != nil {
			return fmt.Errorf("cannot marshal event %q in topic %q: %w", event.State.ID, event.Topic, err)
		}
		return tx.Put(event.State.ID, data)
	})
}

func (s *Service) clearHistory(event *alert.Event) error {
	// clear on-disk EventStates, but leave the in-memory history
	return s.topicsStore.Update(func(tx storage.Tx) error {
		tx = tx.Bucket([]byte(event.Topic))
		if tx == nil {
			return nil
		}
		// Clear previous alert on recovery reset/recovery.
		if err := tx.Delete(event.State.ID); err != nil {
			return fmt.Errorf("cannot delete alert %q in topic %q on reset: %w", event.State.ID, event.Topic, err)
		}
		return nil
	})
}

func (s *Service) restoreClosedTopic(topic string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closedTopics[topic] {
		// Topic already restored
		return nil
	}
	if err := s.restoreTopic(topic); err != nil {
		return err
	}
	// Topic no longer closed
	delete(s.closedTopics, topic)
	return nil
}

// restoreTopic restores a topic's state from the storage and registers any handlers.
// Caller must have lock to call.
func (s *Service) restoreTopic(topic string) error {
	err := s.topicsStore.View(func(tx storage.ReadOnlyTx) error {
		q, err := tx.Bucket([]byte(topic)).List("")
		if err != nil {
			return fmt.Errorf("cannot open database bucket for topic %q: %w", topic, err)
		}
		eventStates := make(map[string]*alert.EventState, len(q))
		es := &EventState{} //create a buffer to hold the unmarshalled EventState
		for _, b := range q {
			lex := jlexer.Lexer{
				Data:              b.Value,
				UseMultipleErrors: false,
			}
			es.UnmarshalEasyJSON(&lex)
			if err := lex.Error(); err != nil {
				return fmt.Errorf("failed to unmarshal event for topic %q: %w", topic, err)
			}
			eventStates[b.Key] = es.AlertEventState(b.Key)
			es.Reset()
		}
		s.topics.RestoreTopicNoCopy(topic, eventStates)
		return nil
	})
	if err != nil {
		return err
	}

	// Re-Register all handlers
	for _, h := range s.handlers[topic] {
		s.topics.RegisterHandler(topic, h.Handler)
	}
	return nil
}

func (s *Service) RestoreTopic(topic string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.restoreTopic(topic)
}

func (s *Service) CloseTopic(topic string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Delete running topic
	s.topics.DeleteTopic(topic)
	s.closedTopics[topic] = true

	return nil
}

func (s *Service) DeleteTopic(topic string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.closedTopics, topic)
	s.topics.DeleteTopic(topic)
	return s.topicsStore.Update(func(tx storage.Tx) error {
		return tx.Delete(topic)
	})
}

func (s *Service) UpdateEvent(topic string, event alert.EventState) error {
	s.topics.UpdateEvent(topic, event)
	return s.persistEventState(alert.Event{
		Topic: topic,
		State: event,
	})
}

func (s *Service) RegisterAnonHandler(topic string, h alert.Handler) {
	s.topics.RegisterHandler(topic, h)
}

func (s *Service) DeregisterAnonHandler(topic string, h alert.Handler) {
	s.topics.DeregisterHandler(topic, h)
}

// loadHandlerSpec initializes a spec that already exists.
// Caller must have the write lock.
func (s *Service) loadHandlerSpec(spec HandlerSpec) error {
	h, err := s.createHandlerFromSpec(spec)
	if err != nil {
		return err
	}

	s.setTopicHandler(spec.Topic, spec.ID, h)
	s.topics.RegisterHandler(spec.Topic, h.Handler)
	return nil
}

func (s *Service) RegisterHandlerSpec(spec HandlerSpec) error {
	if err := spec.Validate(); err != nil {
		return err
	}

	h, err := s.createHandlerFromSpec(spec)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.handlers[spec.Topic][spec.ID]
	if ok {
		return fmt.Errorf("cannot register handler, handler with ID %q already exists", spec.ID)
	}

	// Persist handler spec
	if err := s.specsDAO.Create(spec); err != nil {
		return err
	}

	s.setTopicHandler(spec.Topic, spec.ID, h)

	s.topics.RegisterHandler(spec.Topic, h.Handler)
	return nil
}

type closer interface {
	Close()
}

func (s *Service) DeregisterHandlerSpec(topic, handler string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	h, ok := s.handlers[topic][handler]

	if ok {
		// Delete handler spec
		if err := s.specsDAO.Delete(topic, handler); err != nil {
			return err
		}
		s.topics.DeregisterHandler(topic, h.Handler)

		if ha, ok := h.Handler.(closer); ok {
			ha.Close()
		}

		delete(s.handlers[h.Spec.Topic], handler)
	}
	return nil
}

func (s *Service) UpdateHandlerSpec(oldSpec, newSpec HandlerSpec) error {
	if err := newSpec.Validate(); err != nil {
		return err
	}
	if newSpec.Topic != oldSpec.Topic {
		return errors.New("cannot change topic in update")
	}
	topic := newSpec.Topic
	newH, err := s.createHandlerFromSpec(newSpec)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	oldH := s.handlers[topic][oldSpec.ID]

	// Persist new handler specs
	if newSpec.ID == oldSpec.ID {
		if err := s.specsDAO.Replace(newSpec); err != nil {
			return err
		}
	} else {
		if err := s.specsDAO.Create(newSpec); err != nil {
			return err
		}
		if err := s.specsDAO.Delete(oldSpec.Topic, oldSpec.ID); err != nil {
			return err
		}
	}

	delete(s.handlers[topic], oldSpec.ID)
	s.setTopicHandler(newSpec.Topic, newSpec.ID, newH)

	s.topics.ReplaceHandler(topic, oldH.Handler, newH.Handler)
	return nil
}

// TopicState returns the state for the specified topic.
func (s *Service) TopicState(topic string) (alert.TopicState, bool, error) {
	t, ok := s.topics.Topic(topic)
	if !ok {
		return alert.TopicState{}, false, nil
	}
	return t.State(), true, nil
}

// TopicStates returns the max alert level for each topic matching 'pattern', not returning
// any topics with max alert levels less severe than 'minLevel'
func (s *Service) TopicStates(pattern string, minLevel alert.Level) (map[string]alert.TopicState, error) {
	return s.topics.TopicState(pattern, minLevel), nil
}

// EventState returns the current state of the event.
func (s *Service) EventState(topic, event string) (alert.EventState, bool, error) {
	t, ok := s.topics.Topic(topic)

	if !ok {
		return alert.EventState{}, false, nil
	}
	state, ok := t.EventState(event)
	return state, ok, nil
}

// EventStates returns the current state of events for the specified topic.
// Only events greater or equal to minLevel will be returned
func (s *Service) EventStates(topic string, minLevel alert.Level) (map[string]alert.EventState, error) {
	t, ok := s.topics.Topic(topic)
	if !ok {
		return nil, fmt.Errorf("unknown topic %q", topic)
	}
	return t.EventStates(minLevel), nil
}

func (s *Service) HandlerSpec(topic, handler string) (HandlerSpec, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	h, ok := s.handlers[topic][handler]
	if !ok {
		return HandlerSpec{}, false, nil
	}
	return h.Spec, true, nil
}

func (s *Service) HandlerSpecs(topic, pattern string) ([]HandlerSpec, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	handlers := make([]HandlerSpec, 0, len(s.handlers))
	for id, h := range s.handlers[topic] {
		if alert.PatternMatch(pattern, id) {
			handlers = append(handlers, h.Spec)
		}
	}
	return handlers, nil
}

func decodeOptions(options map[string]interface{}, c interface{}) error {
	dec, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		ErrorUnused: true,
		Result:      c,
		DecodeHook:  decodeStringToTextUnmarshaler,
	})
	if err != nil {
		return errors.Wrap(err, "failed to initialize mapstructure decoder")
	}
	if err := dec.Decode(options); err != nil {
		return errors.Wrapf(err, "failed to decode options into %T", c)
	}
	return nil
}

var textUnmarshalerType = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()

// decodeStringToTextUnmarshaler will decode a string value into any type
// that implements the encoding.TextUnmarshaler interface.
func decodeStringToTextUnmarshaler(f, t reflect.Type, data interface{}) (interface{}, error) {
	if f.Kind() != reflect.String {
		return data, nil
	}
	isPtr := true
	if t.Kind() != reflect.Ptr {
		isPtr = false
		t = reflect.PointerTo(t)
	}
	if t.Implements(textUnmarshalerType) {
		value := reflect.New(t.Elem())
		tum := value.Interface().(encoding.TextUnmarshaler)
		str := data.(string)
		err := tum.UnmarshalText([]byte(str))
		if err != nil {
			return nil, err
		}

		if isPtr {
			return value.Interface(), nil
		}
		return reflect.Indirect(value).Interface(), nil
	}
	return data, nil
}

var ErrHandlerDIsabled = errors.New("handler disabled")

func (s *Service) createHandlerFromSpec(spec HandlerSpec) (handler, error) {

	if _, ok := s.disabled[spec.Kind]; ok {
		s.diag.Error(fmt.Sprintf("handler '%s' is disabled", spec.Kind), ErrHandlerDIsabled)
		return handler{}, nil
	}

	var h alert.Handler
	var err error
	ctx := []keyvalue.T{
		keyvalue.KV("handler", spec.ID),
		keyvalue.KV("topic", spec.Topic),
	}
	switch spec.Kind {
	case "aggregate":
		c := newDefaultAggregateHandlerConfig(s.EventCollector)
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		handlerDiag := s.diag.WithHandlerContext(ctx...)
		h, err = NewAggregateHandler(c, handlerDiag)
		if err != nil {
			return handler{}, err
		}
	case "alerta":
		c := s.AlertaService.DefaultHandlerConfig()
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h, err = s.AlertaService.Handler(c, ctx...)
		if err != nil {
			return handler{}, err
		}
		h = newExternalHandler(h)
	case "bigpanda":
		c := bigpanda.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h, err = s.BigPandaService.Handler(c, ctx...)
		if err != nil {
			return handler{}, err
		}
		h = newExternalHandler(h)
	case "discord":
		c := discord.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h, err = s.DiscordService.Handler(c, ctx...)
		if err != nil {
			return handler{}, err
		}
		h = newExternalHandler(h)
	case "exec":
		c := ExecHandlerConfig{
			Commander: s.Commander,
		}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		handlerDiag := s.diag.WithHandlerContext(ctx...)
		h = NewExecHandler(c, handlerDiag)
		h = newExternalHandler(h)
	case "hipchat":
		c := hipchat.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h = s.HipChatService.Handler(c, ctx...)
		h = newExternalHandler(h)
	case "kafka":
		c := kafka.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h, err = s.KafkaService.Handler(c, ctx...)
		if err != nil {
			return handler{}, err
		}
		h = newExternalHandler(h)
	case "log":
		c := DefaultLogHandlerConfig()
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		handlerDiag := s.diag.WithHandlerContext(ctx...)
		h, err = NewLogHandler(c, handlerDiag)
		if err != nil {
			return handler{}, err
		}
		h = newExternalHandler(h)
	case "mqtt":
		c := mqtt.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h, err = s.MQTTService.Handler(c, ctx...)
		if err != nil {
			return handler{}, err
		}
		h = newExternalHandler(h)
	case "opsgenie":
		c := opsgenie.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h = s.OpsGenieService.Handler(c, ctx...)
		h = newExternalHandler(h)
	case "opsgenie2":
		c := opsgenie2.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h = s.OpsGenie2Service.Handler(c, ctx...)
		h = newExternalHandler(h)
	case "pagerduty":
		c := pagerduty.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h = s.PagerDutyService.Handler(c, ctx...)
		h = newExternalHandler(h)
	case "pagerduty2":
		c := pagerduty2.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h, err = s.PagerDuty2Service.Handler(c, ctx...)
		if err != nil {
			return handler{}, err
		}
		h = newExternalHandler(h)
	case "pushover":
		c := pushover.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h = s.PushoverService.Handler(c, ctx...)
		h = newExternalHandler(h)
	case "post":
		c := httppost.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h, err = s.HTTPPostService.Handler(c, ctx...)
		if err != nil {
			return handler{}, err
		}
		h = newExternalHandler(h)
	case "publish":
		c := PublishHandlerConfig{
			ec: s.EventCollector,
		}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		handlerDiag := s.diag.WithHandlerContext(ctx...)
		h = NewPublishHandler(c, handlerDiag)
	case "sensu":
		c := sensu.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h, err = s.SensuService.Handler(c, ctx...)
		if err != nil {
			return handler{}, err
		}
		h = newExternalHandler(h)
	case "servicenow":
		c := servicenow.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h = s.ServiceNowService.Handler(c, ctx...)
		h = newExternalHandler(h)
	case "slack":
		c := slack.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h = s.SlackService.Handler(c, ctx...)
		h = newExternalHandler(h)
	case "smtp":
		c := smtp.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h = s.SMTPService.Handler(c, ctx...)
		h = newExternalHandler(h)
	case "snmptrap":
		c := snmptrap.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h, err = s.SNMPTrapService.Handler(c, ctx...)
		if err != nil {
			return handler{}, err
		}
		h = newExternalHandler(h)
	case "talk":
		h = s.TalkService.Handler(ctx...)
		h = newExternalHandler(h)
	case "tcp":
		c := TCPHandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		handlerDiag := s.diag.WithHandlerContext(ctx...)
		h = NewTCPHandler(c, handlerDiag)
		h = newExternalHandler(h)
	case "teams":
		c := teams.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h = s.TeamsService.Handler(c, ctx...)
		h = newExternalHandler(h)
	case "telegram":
		c := telegram.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h = s.TelegramService.Handler(c, ctx...)
		h = newExternalHandler(h)
	case "victorops":
		c := victorops.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h = s.VictorOpsService.Handler(c, ctx...)
		h = newExternalHandler(h)
	case "zenoss":
		c := zenoss.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h = s.ZenossService.Handler(c, ctx...)
		h = newExternalHandler(h)
	default:
		err = fmt.Errorf("unsupported action kind %q", spec.Kind)
	}
	if h == nil && err != nil {
		return handler{}, err
	}
	if spec.Match != "" {
		// Wrap handler in match handler
		handlerDiag := s.diag.WithHandlerContext(ctx...)
		if h == nil {
			panic("handler is nil, this should not happen")
		}
		var err2 error
		h, err2 = newMatchHandler(spec.Match, h, handlerDiag)
		if err2 != nil {
			return handler{Spec: spec, Handler: h}, err2
		}
	}
	return handler{Spec: spec, Handler: h}, err
}

func (s *Service) IsInhibited(name string, tags models.Tags) bool {
	return s.inhibitorLookup.IsInhibited(name, tags)
}
func (s *Service) AddInhibitor(in *alert.Inhibitor) {
	s.inhibitorLookup.AddInhibitor(in)
}
func (s *Service) RemoveInhibitor(in *alert.Inhibitor) {
	s.inhibitorLookup.RemoveInhibitor(in)
}
