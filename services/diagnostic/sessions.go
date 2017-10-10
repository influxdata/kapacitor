package diagnostic

import (
	"bytes"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/influxdata/kapacitor/uuid"
)

type SessionsStore interface {
	Create(w http.ResponseWriter, contentType string, level Level, tags []tag) *Session
	Delete(s *Session) error
	Each(func(*Session))
	SetDiagnostic(Diagnostic)
}

type sessionsStore struct {
	mu       sync.RWMutex
	sessions map[uuid.UUID]*Session

	diag Diagnostic
}

func (kv *sessionsStore) SetDiagnostic(d Diagnostic) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.diag = d

}

func (kv *sessionsStore) Create(w http.ResponseWriter, contentType string, logLevel Level, tags []tag) *Session {
	kv.mu.Lock()

	wf, ok := w.(WriteFlusher)
	if !ok {
		wf = &noopWriteFlusher{w: w}
	}

	s := &Session{
		id:          uuid.New(),
		tags:        tags,
		w:           wf,
		level:       logLevel,
		contentType: contentType,
	}

	kv.sessions[s.id] = s

	// Need explicit unlock before call to CreatedLogSession
	kv.mu.Unlock()
	if kv.diag != nil {
		kv.diag.CreatedLogSession(s.id, contentType, tags)
	}

	return s
}

func (kv *sessionsStore) Delete(s *Session) error {
	kv.mu.Lock()

	if s == nil {
		return errors.New("session is nil")
	}

	delete(kv.sessions, s.id)

	// Need explicit unlock before call to CreatedLogSession
	kv.mu.Unlock()
	if kv.diag != nil {
		kv.diag.DeletedLogSession(s.id, s.contentType, s.tags)
	}

	return nil
}

func (kv *sessionsStore) Each(fn func(*Session)) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	for _, s := range kv.sessions {
		fn(s)
	}
}

type tag struct {
	key   string
	value string
}

type Session struct {
	mu sync.Mutex
	id uuid.UUID

	level Level

	tags []tag

	buf         bytes.Buffer
	w           WriteFlusher
	contentType string
}

func (s *Session) Error(msg string, context, fields []Field) {
	if s.level <= ErrorLevel && match(s.tags, msg, "error", context, fields) {
		s.Log(time.Now(), "error", msg, context, fields)
	}
}

func (s *Session) Warn(msg string, context, fields []Field) {
	if s.level <= WarnLevel && match(s.tags, msg, "warn", context, fields) {
		s.Log(time.Now(), "warn", msg, context, fields)
	}
}

func (s *Session) Debug(msg string, context, fields []Field) {
	if s.level <= DebugLevel && match(s.tags, msg, "debug", context, fields) {
		s.Log(time.Now(), "debug", msg, context, fields)
	}
}

func (s *Session) Info(msg string, context, fields []Field) {
	if s.level <= InfoLevel && match(s.tags, msg, "info", context, fields) {
		s.Log(time.Now(), "info", msg, context, fields)
	}
}

func (s *Session) Log(now time.Time, level, msg string, context, fields []Field) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch s.contentType {
	case "application/json":
		writeJSON(&s.buf, now, level, msg, context, fields)
	default:
		writeLogfmt(&s.buf, now, level, msg, context, fields)
	}
	s.w.Write(s.buf.Bytes())
	s.buf.Reset()
	s.w.Flush()
}

func match(tags []tag, msg, level string, context, fields []Field) bool {
	ctr := 0
Loop:
	for _, t := range tags {
		if t.key == "msg" && t.value == msg {
			ctr++
			continue Loop
		}
		if t.key == "lvl" && t.value == level {
			ctr++
			continue Loop
		}
		for _, c := range context {
			if c.Match(t.key, t.value) {
				ctr++
				continue Loop
			}
		}
		for _, f := range fields {
			if f.Match(t.key, t.value) {
				ctr++
				continue Loop
			}
		}
	}

	return len(tags) == ctr
}

type noopWriteFlusher struct {
	w http.ResponseWriter
}

func (h *noopWriteFlusher) Write(buf []byte) (int, error) {
	return h.w.Write(buf)
}
func (h *noopWriteFlusher) Flush() {
}
