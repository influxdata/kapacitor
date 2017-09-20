package diagnostic

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func writeString(w Writer, s string) (n int, err error) {
	var m int
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
	WriteJSONTo(w Writer) (n int64, err error)
	WriteLogfmtTo(w Writer) (n int64, err error)
	Match(key, value string) bool
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

func (s StringField) Match(key, value string) bool {
	return bytes.Equal(s.key, []byte(key)) && s.value == value
}

func (s StringField) WriteLogfmtTo(w Writer) (n int64, err error) {
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

func (s StringField) WriteJSONTo(w Writer) (n int64, err error) {
	var m int

	m, err = w.WriteString(strconv.Quote(string(s.key)))
	n += int64(m)
	if err != nil {
		return
	}

	err = w.WriteByte(':')
	n += 1
	if err != nil {
		return
	}

	m, err = w.WriteString(strconv.Quote(s.value))
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

func (s StringerField) Match(key, value string) bool {
	return bytes.Equal(s.key, []byte(key)) && s.value.String() == value
}

func (s StringerField) WriteLogfmtTo(w Writer) (n int64, err error) {
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

func (s StringerField) WriteJSONTo(w Writer) (n int64, err error) {
	var m int

	m, err = w.WriteString(strconv.Quote(string(s.key)))
	n += int64(m)
	if err != nil {
		return
	}

	err = w.WriteByte(':')
	n += 1
	if err != nil {
		return
	}

	m, err = w.WriteString(strconv.Quote(s.value.String()))
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

func (s GroupedField) Match(key, value string) bool {
	return false
}

func (s GroupedField) WriteLogfmtTo(w Writer) (n int64, err error) {
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

		k, err = value.WriteLogfmtTo(w)
		n += k
		if err != nil {
			return
		}

	}

	return
}

func (s GroupedField) WriteJSONTo(w Writer) (n int64, err error) {
	var m int
	var k int64

	m, err = w.WriteString(strconv.Quote(string(s.key)))
	n += int64(m)
	if err != nil {
		return
	}

	err = w.WriteByte(':')
	n += 1
	if err != nil {
		return
	}

	err = w.WriteByte('{')
	n += 1
	if err != nil {
		return
	}

	for i, value := range s.values {
		if i != 0 {
			err = w.WriteByte(',')
			n += 1
			if err != nil {
				return
			}
		}

		k, err = value.WriteJSONTo(w)
		n += k
		if err != nil {
			return
		}

	}

	err = w.WriteByte('}')
	n += 1
	if err != nil {
		return
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

func (s StringsField) Match(key, value string) bool {
	return false
}

func (s StringsField) WriteLogfmtTo(w Writer) (n int64, err error) {
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

func (s StringsField) WriteJSONTo(w Writer) (n int64, err error) {
	var m int

	if len(s.values) == 0 {
		return String(string(s.key), "nil").WriteJSONTo(w)
	}

	for i, value := range s.values {
		if i != 0 {
			err = w.WriteByte(',')
			n += 1
			if err != nil {
				return
			}
		}

		m, err = w.WriteString(fmt.Sprintf("\"%s_%v\"", string(s.key), i))
		n += int64(m)
		if err != nil {
			return
		}

		err = w.WriteByte(':')
		n += 1
		if err != nil {
			return
		}

		m, err = w.WriteString(strconv.Quote(value))
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

func (s IntField) Match(key, value string) bool {
	return false
}

func (s IntField) WriteLogfmtTo(w Writer) (n int64, err error) {
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

func (s IntField) WriteJSONTo(w Writer) (n int64, err error) {
	var m int

	m, err = w.WriteString(strconv.Quote(string(s.key)))
	n += int64(m)
	if err != nil {
		return
	}

	err = w.WriteByte(':')
	n += 1
	if err != nil {
		return
	}

	m, err = w.WriteString(strconv.Itoa(s.value))
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

func (s Int64Field) Match(key, value string) bool {
	return false
}

func (s Int64Field) WriteLogfmtTo(w Writer) (n int64, err error) {
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

func (s Int64Field) WriteJSONTo(w Writer) (n int64, err error) {
	var m int

	m, err = w.WriteString(strconv.Quote(string(s.key)))
	n += int64(m)
	if err != nil {
		return
	}

	err = w.WriteByte(':')
	n += 1
	if err != nil {
		return
	}

	m, err = w.WriteString(strconv.FormatInt(s.value, 10))
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

func (s Float64Field) Match(key, value string) bool {
	return false
}

func (s Float64Field) WriteLogfmtTo(w Writer) (n int64, err error) {
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

func (s Float64Field) WriteJSONTo(w Writer) (n int64, err error) {
	var m int

	m, err = w.WriteString(strconv.Quote(string(s.key)))
	n += int64(m)
	if err != nil {
		return
	}

	err = w.WriteByte(':')
	n += 1
	if err != nil {
		return
	}

	m, err = w.WriteString(strconv.FormatFloat(s.value, 'f', -1, 64))
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

func (s BoolField) Match(key, value string) bool {
	return false
}

func (s BoolField) WriteLogfmtTo(w Writer) (n int64, err error) {
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

func (s BoolField) WriteJSONTo(w Writer) (n int64, err error) {
	var m int

	m, err = w.WriteString(strconv.Quote(string(s.key)))
	n += int64(m)
	if err != nil {
		return
	}

	err = w.WriteByte(':')
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

func (s ErrorField) Match(key, value string) bool {
	return false
}

func (s ErrorField) WriteLogfmtTo(w Writer) (n int64, err error) {
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

func (s ErrorField) WriteJSONTo(w Writer) (n int64, err error) {
	var m int

	m, err = w.Write([]byte("\"err\""))
	n += int64(m)
	if err != nil {
		return
	}

	err = w.WriteByte(':')
	n += 1
	if err != nil {
		return
	}

	errStr := "nil"
	if s.err != nil {
		errStr = s.err.Error()
	}

	m, err = w.WriteString(strconv.Quote(errStr))
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

func (s TimeField) Match(key, value string) bool {
	return false
}

func (s TimeField) WriteLogfmtTo(w Writer) (n int64, err error) {
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

func (s TimeField) WriteJSONTo(w Writer) (n int64, err error) {
	var m int

	m, err = w.WriteString(strconv.Quote(string(s.key)))
	n += int64(m)
	if err != nil {
		return
	}

	err = w.WriteByte(':')
	n += 1
	if err != nil {
		return
	}

	m, err = w.WriteString(strconv.Quote(s.value.Format(time.RFC3339Nano)))
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

func (s DurationField) Match(key, value string) bool {
	return false
}

func (s DurationField) WriteLogfmtTo(w Writer) (n int64, err error) {
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

func (s DurationField) WriteJSONTo(w Writer) (n int64, err error) {
	var m int

	m, err = w.WriteString(strconv.Quote(string(s.key)))
	n += int64(m)
	if err != nil {
		return
	}

	err = w.WriteByte(':')
	n += 1
	if err != nil {
		return
	}

	m, err = w.WriteString(strconv.Quote(s.value.String()))
	n += int64(m)
	if err != nil {
		return
	}

	return
}
