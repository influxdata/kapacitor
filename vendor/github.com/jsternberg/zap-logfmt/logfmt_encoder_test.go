package zaplogfmt

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/zap"
	"github.com/uber-go/zap/spywrite"
)

var epoch = time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)

func newLogfmtEncoder(opts ...LogfmtOption) *logfmtEncoder {
	return NewLogfmtEncoder(opts...).(*logfmtEncoder)
}

func withLogfmtEncoder(f func(*logfmtEncoder)) {
	enc := newLogfmtEncoder()
	f(enc)
	enc.Free()
}

type loggable struct{ bool }

func (l loggable) MarshalLog(kv zap.KeyValue) error {
	if !l.bool {
		return errors.New("can't marshal")
	}
	kv.AddString("loggable", "yes")
	return nil
}

func assertLogfmtOutput(t testing.TB, desc string, expected string, f func(zap.Encoder)) {
	withLogfmtEncoder(func(enc *logfmtEncoder) {
		f(enc)
		assert.Equal(t, expected, string(enc.bytes), "Unexpected encoder output after adding a %s.", desc)
	})
	withLogfmtEncoder(func(enc *logfmtEncoder) {
		enc.AddString("foo", "bar")
		f(enc)
		expectedPrefix := "foo=bar"
		if expected != "" {
			// If we expect output, it should be space-separated from the previous
			// field.
			expectedPrefix += " "
		}
		assert.Equal(t, expectedPrefix+expected, string(enc.bytes), "Unexpected encoder output after adding a %s as a second field.", desc)
	})
}

func TestLogfmtEncoderFields(t *testing.T) {
	tests := []struct {
		desc     string
		expected string
		f        func(zap.Encoder)
	}{
		{"string", `k=v`, func(e zap.Encoder) { e.AddString("k", "v") }},
		{"string", `k=`, func(e zap.Encoder) { e.AddString("k", "") }},
		{"string", `k\=v\`, func(e zap.Encoder) { e.AddString(`k\`, `v\`) }},
		{"bool", `k=true`, func(e zap.Encoder) { e.AddBool("k", true) }},
		{"bool", `k=false`, func(e zap.Encoder) { e.AddBool("k", false) }},
		{"bool", `k\=true`, func(e zap.Encoder) { e.AddBool(`k\`, true) }},
		{"int", `k=42`, func(e zap.Encoder) { e.AddInt("k", 42) }},
		{"int", `k\=42`, func(e zap.Encoder) { e.AddInt(`k\`, 42) }},
		{"int64", fmt.Sprintf(`k=%d`, math.MaxInt64), func(e zap.Encoder) { e.AddInt64("k", math.MaxInt64) }},
		{"int64", fmt.Sprintf(`k=%d`, math.MinInt64), func(e zap.Encoder) { e.AddInt64("k", math.MinInt64) }},
		{"int64", fmt.Sprintf(`k\=%d`, math.MaxInt64), func(e zap.Encoder) { e.AddInt64(`k\`, math.MaxInt64) }},
		{"uint", `k=42`, func(e zap.Encoder) { e.AddUint("k", 42) }},
		{"uint", `k\=42`, func(e zap.Encoder) { e.AddUint(`k\`, 42) }},
		{"uint64", fmt.Sprintf(`k=%d`, uint64(math.MaxUint64)), func(e zap.Encoder) { e.AddUint64("k", math.MaxUint64) }},
		{"uint64", fmt.Sprintf(`k\=%d`, uint64(math.MaxUint64)), func(e zap.Encoder) { e.AddUint64(`k\`, math.MaxUint64) }},
		{"uintptr", fmt.Sprintf(`k=%d`, uint64(math.MaxUint64)), func(e zap.Encoder) { e.AddUintptr("k", uintptr(math.MaxUint64)) }},
		{"float64", `k=1`, func(e zap.Encoder) { e.AddFloat64("k", 1.0) }},
		{"float64", `k\=1`, func(e zap.Encoder) { e.AddFloat64(`k\`, 1.0) }},
		{"float64", `k=10000000000`, func(e zap.Encoder) { e.AddFloat64("k", 1e10) }},
		{"marshaler", `k.loggable=yes`, func(e zap.Encoder) {
			assert.NoError(t, e.AddMarshaler("k", loggable{true}), "Unexpected error calling MarshalLog.")
		}},
		/*
			{"marshaler", `k\.loggable=yes`, func(e zap.Encoder) {
				assert.NoError(t, e.AddMarshaler(`k\`, loggable{true}), "Unexpected error calling MarshalLog.")
			}},
			{"marshaler", ``, func(e zap.Encoder) {
				assert.Error(t, e.AddMarshaler("k", loggable{false}), "Expected an error calling MarshalLog.")
			}},
		*/
	}

	for _, tt := range tests {
		assertLogfmtOutput(t, tt.desc, tt.expected, tt.f)
	}
}

func TestLogfmtWriteEntryFailure(t *testing.T) {
	withLogfmtEncoder(func(enc *logfmtEncoder) {
		tests := []struct {
			sink io.Writer
			msg  string
		}{
			{spywrite.FailWriter{}, "Expected an error when writing to sink fails."},
			{spywrite.ShortWriter{}, "Expected an error on partial writes to sink."},
		}
		for _, tt := range tests {
			err := enc.WriteEntry(tt.sink, "hello", zap.InfoLevel, time.Unix(0, 0))
			assert.Error(t, err, tt.msg)
		}
	})
}

func TestLogfmtOptions(t *testing.T) {
	root := NewLogfmtEncoder(
		MessageKey("the-message"),
		LevelString("the-level"),
		RFC3339Formatter("the-timestamp"),
	)

	for _, enc := range []zap.Encoder{root, root.Clone()} {
		buf := &bytes.Buffer{}
		enc.WriteEntry(buf, "fake msg", zap.DebugLevel, epoch)
		assert.Equal(
			t,
			`the-timestamp=1970-01-01T00:00:00Z the-level=debug the-message="fake msg"`+"\n",
			buf.String(),
			"Unexpected log output with non-default encoder options.",
		)
	}
}
