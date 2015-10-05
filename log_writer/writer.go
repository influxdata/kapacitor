package log_writer

import (
	"fmt"
	"io"
	"log"
	"strings"
)

type Level int

const (
	_ Level = iota
	DEBUG
	INFO
	WARN
	ERROR
)

const Delimeter = '@'

var Levels = map[byte]Level{
	'D': DEBUG,
	'I': INFO,
	'W': WARN,
	'E': ERROR,
}

// The global log level.
var LogLevel = INFO

// name to Level mappings
var levels = map[string]Level{
	"DEBUG": DEBUG,
	"INFO":  INFO,
	"WARN":  WARN,
	"ERROR": ERROR,
}

// Set the log level via a string name. To set it directly use 'LogLevel'.
func SetLevel(level string) error {
	l := levels[strings.ToUpper(level)]
	if l > 0 {
		LogLevel = l
	} else {
		return fmt.Errorf("invalid log level: %q", level)
	}
	return nil
}

// Implements io.Writer. Checks first byte of write for log level
// and drops the log if necessary
type Writer struct {
	start int
	w     io.Writer
}

func New(w io.Writer, prefix string, flag int) *log.Logger {
	return log.New(NewWriter(w), prefix, flag)
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{-1, w}
}

func (w *Writer) Write(buf []byte) (int, error) {
	if len(buf) > 0 {
		if w.start == -1 {
			// Find start of message index
			for i, c := range buf {
				if c == Delimeter && i > 0 {
					l := buf[i-1]
					level := Levels[l]
					if level > 0 {
						w.start = i - 1
						break
					}
				}
			}
			if w.start == -1 {
				return w.w.Write([]byte("log messages must have L@ prefix where L is one of 'D', 'I', 'W', 'E'\n"))
			}
		}
		l := Levels[buf[w.start]]
		if l >= LogLevel {
			n, err := w.w.Write(buf[:w.start])
			if err != nil {
				return n, err
			}
			return w.w.Write(buf[w.start+2:])
		}
	}
	return 0, nil
}
