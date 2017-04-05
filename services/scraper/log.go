package scraper

import (
	"log"

	plog "github.com/prometheus/common/log"
)

// Logger wraps kapacitor logging for prometheus
type Logger struct {
	*log.Logger
}

// NewLogger wraps a logger to be used for prometheus
func NewLogger(l *log.Logger) *Logger {
	return &Logger{
		Logger: l,
	}
}

// Debug logs a message at level Debug on the standard logger.
func (l *Logger) Debug(v ...interface{}) {
	l.Logger.Print("D! ", v)
}

// Debugln logs a message at level Debug on the standard logger.
func (l *Logger) Debugln(v ...interface{}) {
	l.Logger.Println("D! ", v)
}

// Debugf logs a message at level Debug on the standard logger.
func (l *Logger) Debugf(s string, v ...interface{}) {
	l.Logger.Printf("D! "+s, v)
}

// Info logs a message at level Info on the standard logger.
func (l *Logger) Info(v ...interface{}) {
	l.Logger.Print("I! ", v)
}

// Infoln logs a message at level Info on the standard logger.
func (l *Logger) Infoln(v ...interface{}) {
	l.Logger.Println("I! ", v)
}

// Infof logs a message at level Info on the standard logger.
func (l *Logger) Infof(s string, v ...interface{}) {
	l.Logger.Printf("I! "+s, v)
}

// Warn logs a message at level Warn on the standard logger.
func (l *Logger) Warn(v ...interface{}) {
	l.Logger.Print("W! ", v)
}

// Warnln logs a message at level Warn on the standard logger.
func (l *Logger) Warnln(v ...interface{}) {
	l.Logger.Println("W! ", v)
}

// Warnf logs a message at level Warn on the standard logger.
func (l *Logger) Warnf(s string, v ...interface{}) {
	l.Logger.Printf("W! "+s, v)
}

// Error logs a message at level Error on the standard logger.
func (l *Logger) Error(v ...interface{}) {
	l.Logger.Print("E! ", v)
}

// Errorln logs a message at level Error on the standard logger.
func (l *Logger) Errorln(v ...interface{}) {
	l.Logger.Println("E! ", v)
}

// Errorf logs a message at level Error on the standard logger.
func (l *Logger) Errorf(s string, v ...interface{}) {
	l.Logger.Printf("E! "+s, v)
}

// Fatal logs a message at level Fatal on the standard logger.
func (l *Logger) Fatal(v ...interface{}) {
	l.Logger.Fatal(v)
}

// Fatalln logs a message at level Fatal on the standard logger.
func (l *Logger) Fatalln(v ...interface{}) {
	l.Logger.Fatalln(v)
}

// Fatalf logs a message at level Fatal on the standard logger.
func (l *Logger) Fatalf(s string, v ...interface{}) {
	l.Logger.Fatalf(s, v)
}

// With adds a field to the logger.
func (l *Logger) With(key string, value interface{}) plog.Logger {
	return l
}
