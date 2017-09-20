package log

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func writeString(w *bufio.Writer, s string) (n int, err error) {
	var m int
	// TODO: revisit
	if strings.ContainsAny(s, " \"") {
		m, err = w.WriteString(strconv.Quote(s))
		n += m
		if err != nil {
			return
		}
	} else {
		m, err = w.WriteString(s)
		n += m
		if err != nil {
			return
		}
	}

	return
}

type Field interface {
	WriteTo(w *bufio.Writer) (n int64, err error)
}

// String
type StringField struct {
	key   []byte
	value string
}

func String(key string, value string) Field {
	return StringField{
		key:   []byte(key),
		value: value,
	}
}

func (s StringField) WriteTo(w *bufio.Writer) (n int64, err error) {
	var m int

	m, err = w.Write(s.key)
	n += int64(m)
	if err != nil {
		return
	}

	err = w.WriteByte('=')
	n += 1
	if err != nil {
		return
	}

	m, err = writeString(w, s.value)
	n += int64(m)
	if err != nil {
		return
	}

	return
}

// Stringer
type StringerField struct {
	key   []byte
	value fmt.Stringer
}

func Stringer(key string, value fmt.Stringer) Field {
	return StringerField{
		key:   []byte(key),
		value: value,
	}
}

func (s StringerField) WriteTo(w *bufio.Writer) (n int64, err error) {
	var m int

	m, err = w.Write(s.key)
	n += int64(m)
	if err != nil {
		return
	}

	err = w.WriteByte('=')
	n += 1
	if err != nil {
		return
	}

	m, err = writeString(w, s.value.String())
	n += int64(m)
	if err != nil {
		return
	}

	return
}

type GroupedField struct {
	key    []byte
	values []Field
}

func GroupedFields(key string, fields []Field) Field {
	return GroupedField{
		key:    []byte(key),
		values: fields,
	}
}
func (s GroupedField) WriteTo(w *bufio.Writer) (n int64, err error) {
	var m int
	var k int64

	for i, value := range s.values {
		if i != 0 {
			err = w.WriteByte(' ')
			n += 1
			if err != nil {
				return
			}
		}

		m, err = w.Write(s.key)
		n += int64(m)
		if err != nil {
			return
		}

		err = w.WriteByte('_')
		n += 1
		if err != nil {
			return
		}

		k, err = value.WriteTo(w)
		n += k
		if err != nil {
			return
		}

	}

	return
}

// Strings
type StringsField struct {
	key    []byte
	values []string
}

func Strings(key string, values []string) Field {
	return StringsField{
		key:    []byte(key),
		values: values,
	}
}

func (s StringsField) WriteTo(w *bufio.Writer) (n int64, err error) {
	var m int

	for i, value := range s.values {
		if i != 0 {
			err = w.WriteByte(' ')
			n += 1
			if err != nil {
				return
			}
		}

		m, err = w.Write(s.key)
		n += int64(m)
		if err != nil {
			return
		}

		err = w.WriteByte('_')
		n += 1
		if err != nil {
			return
		}

		m, err = w.WriteString(strconv.Itoa(i))
		n += int64(m)
		if err != nil {
			return
		}

		err = w.WriteByte('=')
		n += 1
		if err != nil {
			return
		}

		m, err = writeString(w, value)
		n += int64(m)
		if err != nil {
			return
		}
	}

	return
}

// Int
type IntField struct {
	key   []byte
	value int
}

func Int(key string, value int) Field {
	return IntField{
		key:   []byte(key),
		value: value,
	}
}

func (s IntField) WriteTo(w *bufio.Writer) (n int64, err error) {
	var m int

	m, err = w.Write(s.key)
	n += int64(m)
	if err != nil {
		return
	}

	err = w.WriteByte('=')
	n += 1
	if err != nil {
		return
	}

	m, err = writeString(w, strconv.Itoa(s.value))
	n += int64(m)
	if err != nil {
		return
	}

	return
}

// Int64
type Int64Field struct {
	key   []byte
	value int64
}

func Int64(key string, value int64) Field {
	return Int64Field{
		key:   []byte(key),
		value: value,
	}
}

func (s Int64Field) WriteTo(w *bufio.Writer) (n int64, err error) {
	var m int

	m, err = w.Write(s.key)
	n += int64(m)
	if err != nil {
		return
	}

	err = w.WriteByte('=')
	n += 1
	if err != nil {
		return
	}

	m, err = writeString(w, strconv.FormatInt(s.value, 10))
	n += int64(m)
	if err != nil {
		return
	}

	return
}

// Float64
type Float64Field struct {
	key   []byte
	value float64
}

func Float64(key string, value float64) Field {
	return Float64Field{
		key:   []byte(key),
		value: value,
	}
}

func (s Float64Field) WriteTo(w *bufio.Writer) (n int64, err error) {
	var m int

	m, err = w.Write(s.key)
	n += int64(m)
	if err != nil {
		return
	}

	err = w.WriteByte('=')
	n += 1
	if err != nil {
		return
	}

	m, err = writeString(w, strconv.FormatFloat(s.value, 'f', -1, 64))
	n += int64(m)
	if err != nil {
		return
	}

	return
}

// Bool
type BoolField struct {
	key   []byte
	value bool
}

func Bool(key string, value bool) Field {
	return BoolField{
		key:   []byte(key),
		value: value,
	}
}

func (s BoolField) WriteTo(w *bufio.Writer) (n int64, err error) {
	var m int

	m, err = w.Write(s.key)
	n += int64(m)
	if err != nil {
		return
	}

	err = w.WriteByte('=')
	n += 1
	if err != nil {
		return
	}

	if s.value {
		m, err = w.Write([]byte("true"))
		n += int64(m)
		if err != nil {
			return
		}
	} else {
		m, err = w.Write([]byte("false"))
		n += int64(m)
		if err != nil {
			return
		}
	}

	return
}

// Error
type ErrorField struct {
	err error
}

func Error(err error) Field {
	return ErrorField{
		err: err,
	}
}

func (s ErrorField) WriteTo(w *bufio.Writer) (n int64, err error) {
	var m int

	m, err = w.Write([]byte("err"))
	n += int64(m)
	if err != nil {
		return
	}

	err = w.WriteByte('=')
	n += 1
	if err != nil {
		return
	}

	errStr := "nil"
	if s.err != nil {
		errStr = s.err.Error()
	}

	m, err = writeString(w, errStr)
	n += int64(m)
	if err != nil {
		return
	}

	return
}

// Time
type TimeField struct {
	key   []byte
	value time.Time
}

func Time(key string, value time.Time) Field {
	return TimeField{
		key:   []byte(key),
		value: value,
	}
}

func (s TimeField) WriteTo(w *bufio.Writer) (n int64, err error) {
	var m int

	m, err = w.Write(s.key)
	n += int64(m)
	if err != nil {
		return
	}

	err = w.WriteByte('=')
	n += 1
	if err != nil {
		return
	}

	m, err = writeString(w, s.value.Format(time.RFC3339Nano))
	n += int64(m)
	if err != nil {
		return
	}

	return
}

// Duration
type DurationField struct {
	key   []byte
	value time.Duration
}

func Duration(key string, value time.Duration) Field {
	return DurationField{
		key:   []byte(key),
		value: value,
	}
}

func (s DurationField) WriteTo(w *bufio.Writer) (n int64, err error) {
	var m int

	m, err = w.Write(s.key)
	n += int64(m)
	if err != nil {
		return
	}

	err = w.WriteByte('=')
	n += 1
	if err != nil {
		return
	}

	m, err = writeString(w, s.value.String())
	n += int64(m)
	if err != nil {
		return
	}

	return
}
