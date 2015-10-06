/*
	Provides an io.Writer that filters log messages based on a log level.

	Valid log levels are: DEBUG, INFO, WARN, ERROR.

	Log messages need to begin with a L! where L is one of D, I, W, or E.

	Examples:
		log.Println("D! this is a debug log")
		log.Println("I! this is an info log")
		log.Println("W! this is a warn log")
		log.Println("E! this is an error log")

	Simply pass a instance of wlog.Writer to log.New or use the helper wlog.New function.

	The log level can be changed via the LogLevel variable or the SetLevel function.
*/
package wlog

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

const Delimeter = '!'

var invalidMSG = []byte("log messages must have L! prefix where L is one of 'D', 'I', 'W', 'E'\n")

var Levels = map[byte]Level{
	'D': DEBUG,
	'I': INFO,
	'W': WARN,
	'E': ERROR,
}

// The global and only log level. Log levels are not implemented per writer.
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
				return w.w.Write(invalidMSG)
			}
		}
		l := Levels[buf[w.start]]
		if l >= LogLevel {
			return w.w.Write(buf)
		} else if l == 0 {
			return w.w.Write(invalidMSG)
		}
	}
	return 0, nil
}
