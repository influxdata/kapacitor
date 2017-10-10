package diagnostic

import (
	"bufio"
	"io"
	"strconv"
	"sync"
	"time"
)

const RFC3339Milli = "2006-01-02T15:04:05.000Z07:00"

type Level int

const (
	DebugLevel Level = iota
	InfoLevel
	WarnLevel
	ErrorLevel
)

type Logger interface {
	Error(msg string, ctx ...Field)
	Warn(msg string, ctx ...Field)
	Debug(msg string, ctx ...Field)
	Info(msg string, ctx ...Field)
	With(ctx ...Field) Logger
}

type Writer interface {
	Write([]byte) (int, error)
	WriteByte(byte) error
	WriteString(string) (int, error)
}

type WriteFlusher interface {
	Write([]byte) (int, error)
	Flush()
}

type MultiLogger struct {
	loggers []Logger
}

func NewMultiLogger(loggers ...Logger) *MultiLogger {
	return &MultiLogger{
		loggers: loggers,
	}
}

func (l *MultiLogger) Error(msg string, ctx ...Field) {
	for _, logger := range l.loggers {
		logger.Error(msg, ctx...)
	}
}

func (l *MultiLogger) Warn(msg string, ctx ...Field) {
	for _, logger := range l.loggers {
		logger.Warn(msg, ctx...)
	}
}

func (l *MultiLogger) Debug(msg string, ctx ...Field) {
	for _, logger := range l.loggers {
		logger.Debug(msg, ctx...)
	}
}

func (l *MultiLogger) Info(msg string, ctx ...Field) {
	for _, logger := range l.loggers {
		logger.Info(msg, ctx...)
	}
}

func (l *MultiLogger) With(ctx ...Field) Logger {
	loggers := []Logger{}
	for _, logger := range l.loggers {
		loggers = append(loggers, logger.With(ctx...))
	}

	return NewMultiLogger(loggers...)
}

func defaultLevelF(lvl Level) bool {
	return true
}

type ServerLogger struct {
	mu      *sync.Mutex
	context []Field
	w       *bufio.Writer

	levelMu sync.RWMutex
	levelF  func(lvl Level) bool
}

func NewServerLogger(w io.Writer) *ServerLogger {
	var mu sync.Mutex
	return &ServerLogger{
		mu:     &mu,
		w:      bufio.NewWriter(w),
		levelF: defaultLevelF,
	}
}

// LevelF set on parent applies to self and any future children
func (l *ServerLogger) SetLevelF(f func(Level) bool) {
	l.levelMu.Lock()
	defer l.levelMu.Unlock()
	l.levelF = f
}

func (l *ServerLogger) With(ctx ...Field) Logger {
	l.mu.Lock()
	defer l.mu.Unlock()
	newCtx := make([]Field, len(l.context))
	copy(newCtx, l.context)
	return &ServerLogger{
		mu:      l.mu,
		context: append(newCtx, ctx...),
		w:       l.w,
		levelF:  l.levelF,
	}
}

func (l *ServerLogger) Error(msg string, ctx ...Field) {
	l.levelMu.RLock()
	logLine := l.levelF(ErrorLevel)
	l.levelMu.RUnlock()
	if logLine {
		l.Log(time.Now(), "error", msg, ctx)
	}
}

func (l *ServerLogger) Debug(msg string, ctx ...Field) {
	l.levelMu.RLock()
	logLine := l.levelF(DebugLevel)
	l.levelMu.RUnlock()
	if logLine {
		l.Log(time.Now(), "debug", msg, ctx)
	}
}

func (l *ServerLogger) Warn(msg string, ctx ...Field) {
	l.levelMu.RLock()
	logLine := l.levelF(WarnLevel)
	l.levelMu.RUnlock()
	if logLine {
		l.Log(time.Now(), "warn", msg, ctx)
	}
}

func (l *ServerLogger) Info(msg string, ctx ...Field) {
	l.levelMu.RLock()
	logLine := l.levelF(InfoLevel)
	l.levelMu.RUnlock()
	if logLine {
		l.Log(time.Now(), "info", msg, ctx)
	}
}

// TODO: actually care about errors?
func (l *ServerLogger) Log(now time.Time, level string, msg string, ctx []Field) {
	l.mu.Lock()
	defer l.mu.Unlock()
	writeLogfmt(l.w, now, level, msg, l.context, ctx)
	l.w.Flush()
}

type sessionsLogger struct {
	store   SessionsStore
	context []Field
}

func (s *sessionsLogger) Error(msg string, ctx ...Field) {
	s.store.Each(func(sn *Session) {
		sn.Error(msg, s.context, ctx)
	})
}

func (s *sessionsLogger) Warn(msg string, ctx ...Field) {
	s.store.Each(func(sn *Session) {
		sn.Warn(msg, s.context, ctx)
	})
}

func (s *sessionsLogger) Debug(msg string, ctx ...Field) {
	s.store.Each(func(sn *Session) {
		sn.Debug(msg, s.context, ctx)
	})
}

func (s *sessionsLogger) Info(msg string, ctx ...Field) {
	s.store.Each(func(sn *Session) {
		sn.Info(msg, s.context, ctx)
	})
}

func (s *sessionsLogger) With(ctx ...Field) Logger {
	return &sessionsLogger{
		store:   s.store,
		context: append(s.context, ctx...),
	}
}

// TODO: actually care about errors?
func writeLogfmt(w Writer, now time.Time, level string, msg string, context, fields []Field) {

	writeLogfmtTimestamp(w, now)
	w.WriteByte(' ')
	writeLogfmtLevel(w, level)
	w.WriteByte(' ')
	writeLogfmtMessage(w, msg)

	for _, f := range context {
		w.WriteByte(' ')
		f.WriteLogfmtTo(w)
	}

	for _, f := range fields {
		w.WriteByte(' ')
		f.WriteLogfmtTo(w)
	}

	w.WriteByte('\n')
}

func writeLogfmtTimestamp(w Writer, now time.Time) {
	w.Write([]byte("ts="))
	// TODO: UTC?
	w.WriteString(now.Format(RFC3339Milli))
}

func writeLogfmtLevel(w Writer, lvl string) {
	w.Write([]byte("lvl="))
	w.WriteString(lvl)
}

func writeLogfmtMessage(w Writer, msg string) {
	w.Write([]byte("msg="))
	writeString(w, msg)
}

// TODO: actually care about errors?
func writeJSON(w Writer, now time.Time, level string, msg string, context, fields []Field) {

	w.WriteByte('{')
	writeJSONTimestamp(w, now)
	w.WriteByte(',')
	writeJSONLevel(w, level)
	w.WriteByte(',')
	writeJSONMessage(w, msg)

	for _, f := range context {
		w.WriteByte(',')
		f.WriteJSONTo(w)
	}

	for _, f := range fields {
		w.WriteByte(',')
		f.WriteJSONTo(w)
	}
	w.WriteByte('}')
}

func writeJSONTimestamp(w Writer, now time.Time) {
	w.Write([]byte("\"ts\":"))
	// TODO: UTC?
	w.WriteString(strconv.Quote(now.Format(RFC3339Milli)))
}

func writeJSONLevel(w Writer, lvl string) {
	w.Write([]byte("\"lvl\":"))
	w.WriteString(strconv.Quote(lvl))
}

func writeJSONMessage(w Writer, msg string) {
	w.Write([]byte("\"msg\":"))
	w.WriteString(strconv.Quote(msg))
}
