package zaplogfmt

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/uber-go/zap"
)

const (
	// For JSON-escaping; see logfmtEncoder.safeAddString below.
	_hex = "0123456789abcdef"
	// Initial buffer size for encoders.
	_initialBufSize = 1024
)

var (
	// errNilSink signals that Encoder.WriteEntry was called with a nil WriteSyncer.
	errNilSink = errors.New("can't write encoded message a nil WriteSyncer")

	// Default formatters for logfmt encoders.
	defaultTimeF    = RFC3339Formatter("ts")
	defaultLevelF   = LevelString("level")
	defaultMessageF = MessageKey("msg")

	logfmtPool = sync.Pool{New: func() interface{} {
		return &logfmtEncoder{
			// Pre-allocate a reasonably-sized buffer for each encoder.
			bytes: make([]byte, 0, _initialBufSize),
		}
	}}
)

type logfmtEncoder struct {
	bytes    []byte
	prefix   []byte
	timeF    TimeFormatter
	levelF   LevelFormatter
	messageF MessageFormatter
}

func NewEncoder(options ...LogfmtOption) zap.Encoder {
	enc := logfmtPool.Get().(*logfmtEncoder)
	enc.truncate()

	enc.timeF = defaultTimeF
	enc.levelF = defaultLevelF
	enc.messageF = defaultMessageF
	for _, opt := range options {
		opt.apply(enc)
	}
	return enc
}

func (enc *logfmtEncoder) Free() {
	logfmtPool.Put(enc)
}

func (enc *logfmtEncoder) AddString(key, val string) {
	if enc.addKey(key) {
		if strings.IndexFunc(val, needsQuotedValueRune) != -1 {
			enc.safeAddString(val)
		} else {
			enc.bytes = append(enc.bytes, val...)
		}
	}
}

func (enc *logfmtEncoder) AddBool(key string, val bool) {
	if enc.addKey(key) {
		if val {
			enc.bytes = append(enc.bytes, []byte("true")...)
		} else {
			enc.bytes = append(enc.bytes, []byte("false")...)
		}
	}
}

func (enc *logfmtEncoder) AddInt(key string, val int) {
	enc.AddInt64(key, int64(val))
}

func (enc *logfmtEncoder) AddInt64(key string, val int64) {
	if enc.addKey(key) {
		enc.bytes = strconv.AppendInt(enc.bytes, val, 10)
	}
}

func (enc *logfmtEncoder) AddUint(key string, val uint) {
	enc.AddUint64(key, uint64(val))
}

func (enc *logfmtEncoder) AddUint64(key string, val uint64) {
	if enc.addKey(key) {
		enc.bytes = strconv.AppendUint(enc.bytes, val, 10)
	}
}

func (enc *logfmtEncoder) AddUintptr(key string, val uintptr) {
	enc.AddUint64(key, uint64(val))
}

func (enc *logfmtEncoder) AddFloat64(key string, val float64) {
	if enc.addKey(key) {
		enc.bytes = strconv.AppendFloat(enc.bytes, val, 'f', -1, 64)
	}
}

func (enc *logfmtEncoder) AddMarshaler(key string, obj zap.LogMarshaler) error {
	prefix := append(enc.prefix, key...)
	prefix = append(prefix, '.')
	subenc := logfmtEncoder{
		prefix: prefix,
		bytes:  enc.bytes,
	}
	err := obj.MarshalLog(&subenc)
	enc.bytes = subenc.bytes
	return err
}

func (enc *logfmtEncoder) AddObject(key string, obj interface{}) error {
	enc.AddString(key, fmt.Sprintf("%+v", obj))
	return nil
}

func (enc *logfmtEncoder) Clone() zap.Encoder {
	clone := logfmtPool.Get().(*logfmtEncoder)
	clone.truncate()
	clone.bytes = make([]byte, 0, cap(clone.bytes))
	clone.bytes = append(clone.bytes, enc.bytes...)
	clone.timeF = enc.timeF
	clone.levelF = enc.levelF
	clone.messageF = enc.messageF
	return clone
}

func (enc *logfmtEncoder) WriteEntry(sink io.Writer, msg string, level zap.Level, t time.Time) error {
	if sink == nil {
		return errors.New("can't write encoded message to a nil WriteSyncer")
	}

	final := logfmtPool.Get().(*logfmtEncoder)
	final.truncate()
	enc.timeF(t).AddTo(final)
	enc.levelF(level).AddTo(final)
	enc.messageF(msg).AddTo(final)
	if len(enc.bytes) > 0 {
		final.bytes = append(final.bytes, ' ')
		final.bytes = append(final.bytes, enc.bytes...)
	}
	final.bytes = append(final.bytes, '\n')

	expectedBytes := len(final.bytes)
	n, err := sink.Write(final.bytes)
	final.Free()
	if err != nil {
		return err
	} else if n != expectedBytes {
		return fmt.Errorf("incomplete write: only wrote %v of %v bytes", n, expectedBytes)
	}
	return nil
}

func (enc *logfmtEncoder) truncate() {
	enc.bytes = enc.bytes[:0]
}

func (enc *logfmtEncoder) addKey(key string) bool {
	if len(key) == 0 || strings.IndexFunc(key, needsQuotedValueRune) != -1 {
		return false
	}

	if len(enc.bytes) > 0 {
		enc.bytes = append(enc.bytes, ' ')
	}
	if len(enc.prefix) > 0 {
		enc.bytes = append(enc.bytes, enc.prefix...)
	}
	enc.bytes = append(enc.bytes, key...)
	enc.bytes = append(enc.bytes, '=')
	return true
}

func (enc *logfmtEncoder) safeAddString(val string) {
	enc.bytes = append(enc.bytes, '"')
	start := 0
	for i := 0; i < len(val); {
		if b := val[i]; b < utf8.RuneSelf {
			if 0x20 <= b && b != '\\' && b != '"' {
				i++
				continue
			}
			if start < i {
				enc.bytes = append(enc.bytes, val[start:i]...)
			}
			switch b {
			case '\\', '"':
				enc.bytes = append(enc.bytes, '\\', b)
			case '\n':
				enc.bytes = append(enc.bytes, '\\', 'n')
			case '\r':
				enc.bytes = append(enc.bytes, '\\', 'r')
			case '\t':
				enc.bytes = append(enc.bytes, '\\', 't')
			default:
				enc.bytes = append(enc.bytes, `\u00`...)
				enc.bytes = append(enc.bytes, _hex[b>>4], _hex[b&0xF])
			}
			i++
			start = i
			continue
		}
		c, size := utf8.DecodeRuneInString(val[i:])
		if c == utf8.RuneError {
			if start < i {
				enc.bytes = append(enc.bytes, val[start:i]...)
			}
			enc.bytes = append(enc.bytes, "\ufffd"...)
			i += size
			start = i
			continue
		}
		i += size
	}
	if start < len(val) {
		enc.bytes = append(enc.bytes, val[start:]...)
	}
	enc.bytes = append(enc.bytes, '"')
}

func needsQuotedValueRune(r rune) bool {
	return r <= ' ' || r == '=' || r == '"' || r == utf8.RuneError
}
