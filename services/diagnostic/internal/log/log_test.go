package log_test

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/influxdata/kapacitor/services/diagnostic/internal/log"
)

var defaultTime = time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)

type testStringer string

func (t testStringer) String() string {
	return string(t)
}

func TestLoggerWithoutContext(t *testing.T) {
	now := time.Now()
	nowStr := now.Format(log.RFC3339Milli)
	buf := bytes.NewBuffer(nil)
	l := log.NewLogger(buf)

	tests := []struct {
		name   string
		exp    string
		lvl    string
		msg    string
		fields []log.Field
	}{
		{
			name: "no fields simple message",
			exp:  fmt.Sprintf("ts=%s lvl=error msg=this\n", nowStr),
			lvl:  "error",
			msg:  "this",
		},
		{
			name: "no fields less simple message",
			exp:  fmt.Sprintf("ts=%s lvl=error msg=this/is/a/test\n", nowStr),
			lvl:  "error",
			msg:  "this/is/a/test",
		},
		{
			name: "no fields complex message",
			exp:  fmt.Sprintf("ts=%s lvl=error msg=\"this is \\\" a test/yeah\"\n", nowStr),
			lvl:  "error",
			msg:  "this is \" a test/yeah",
		},
		{
			name: "simple string field",
			exp:  fmt.Sprintf("ts=%s lvl=error msg=test test=this\n", nowStr),
			lvl:  "error",
			msg:  "test",
			fields: []log.Field{
				log.String("test", "this"),
			},
		},
		{
			name: "complex string field",
			exp:  fmt.Sprintf("ts=%s lvl=error msg=test test=\"this is \\\" a test/yeah\"\n", nowStr),
			lvl:  "error",
			msg:  "test",
			fields: []log.Field{
				log.String("test", "this is \" a test/yeah"),
			},
		},
		{
			name: "simple stringer field",
			exp:  fmt.Sprintf("ts=%s lvl=error msg=test test=this\n", nowStr),
			lvl:  "error",
			msg:  "test",
			fields: []log.Field{
				log.Stringer("test", testStringer("this")),
			},
		},
		{
			name: "simple single grouped field",
			exp:  fmt.Sprintf("ts=%s lvl=error msg=test test_a=this\n", nowStr),
			lvl:  "error",
			msg:  "test",
			fields: []log.Field{
				log.GroupedFields("test", []log.Field{
					log.String("a", "this"),
				}),
			},
		},
		{
			name: "simple double grouped field",
			exp:  fmt.Sprintf("ts=%s lvl=error msg=test test_a=this test_b=other\n", nowStr),
			lvl:  "error",
			msg:  "test",
			fields: []log.Field{
				log.GroupedFields("test", []log.Field{
					log.String("a", "this"),
					log.String("b", "other"),
				}),
			},
		},
		{
			name: "simple single strings field",
			exp:  fmt.Sprintf("ts=%s lvl=error msg=test test_0=this\n", nowStr),
			lvl:  "error",
			msg:  "test",
			fields: []log.Field{
				log.Strings("test", []string{"this"}),
			},
		},
		{
			name: "simple double strings field",
			exp:  fmt.Sprintf("ts=%s lvl=error msg=test test_0=this test_1=other\n", nowStr),
			lvl:  "error",
			msg:  "test",
			fields: []log.Field{
				log.Strings("test", []string{"this", "other"}),
			},
		},
		{
			name: "int field",
			exp:  fmt.Sprintf("ts=%s lvl=error msg=test test=10\n", nowStr),
			lvl:  "error",
			msg:  "test",
			fields: []log.Field{
				log.Int("test", 10),
			},
		},
		{
			name: "int64 field",
			exp:  fmt.Sprintf("ts=%s lvl=error msg=test test=10\n", nowStr),
			lvl:  "error",
			msg:  "test",
			fields: []log.Field{
				log.Int64("test", 10),
			},
		},
		{
			name: "float64 field",
			exp:  fmt.Sprintf("ts=%s lvl=error msg=test test=3.1415926535\n", nowStr),
			lvl:  "error",
			msg:  "test",
			fields: []log.Field{
				log.Float64("test", 3.1415926535),
			},
		},
		{
			name: "bool true field",
			exp:  fmt.Sprintf("ts=%s lvl=error msg=test test=true\n", nowStr),
			lvl:  "error",
			msg:  "test",
			fields: []log.Field{
				log.Bool("test", true),
			},
		},
		{
			name: "bool false field",
			exp:  fmt.Sprintf("ts=%s lvl=error msg=test test=false\n", nowStr),
			lvl:  "error",
			msg:  "test",
			fields: []log.Field{
				log.Bool("test", false),
			},
		},
		{
			name: "simple error field",
			exp:  fmt.Sprintf("ts=%s lvl=error msg=test err=this\n", nowStr),
			lvl:  "error",
			msg:  "test",
			fields: []log.Field{
				log.Error(errors.New("this")),
			},
		},
		{
			name: "nil error field",
			exp:  fmt.Sprintf("ts=%s lvl=error msg=test err=nil\n", nowStr),
			lvl:  "error",
			msg:  "test",
			fields: []log.Field{
				log.Error(nil),
			},
		},
		{
			name: "complex error field",
			exp:  fmt.Sprintf("ts=%s lvl=error msg=test err=\"this is \\\" a test/yeah\"\n", nowStr),
			lvl:  "error",
			msg:  "test",
			fields: []log.Field{
				log.Error(errors.New("this is \" a test/yeah")),
			},
		},
		{
			name: "time field",
			exp:  fmt.Sprintf("ts=%s lvl=error msg=test time=%s\n", nowStr, defaultTime.Format(time.RFC3339Nano)),
			lvl:  "error",
			msg:  "test",
			fields: []log.Field{
				log.Time("time", defaultTime),
			},
		},
		{
			name: "duration field",
			exp:  fmt.Sprintf("ts=%s lvl=error msg=test test=1s\n", nowStr),
			lvl:  "error",
			msg:  "test",
			fields: []log.Field{
				log.Duration("test", time.Second),
			},
		},
		{
			name: "two fields",
			exp:  fmt.Sprintf("ts=%s lvl=error msg=test testing=\"that this\" works=1s\n", nowStr),
			lvl:  "error",
			msg:  "test",
			fields: []log.Field{
				log.String("testing", "that this"),
				log.Duration("works", time.Second),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			defer buf.Reset()
			l.Log(now, test.lvl, test.msg, test.fields)
			if exp, got := test.exp, buf.String(); exp != got {
				t.Fatalf("bad log line:\nexp: `%v`\ngot: `%v`", strconv.Quote(exp), strconv.Quote(got))
			}
		})
	}
}

func TestLoggerWithContext(t *testing.T) {
	now := time.Now()
	nowStr := now.Format(log.RFC3339Milli)
	buf := bytes.NewBuffer(nil)
	l := log.NewLogger(buf).With(log.String("a", "tag"), log.Int("id", 10))

	tests := []struct {
		name   string
		exp    string
		lvl    string
		msg    string
		fields []log.Field
	}{
		{
			name: "no fields simple message",
			exp:  fmt.Sprintf("ts=%s lvl=error msg=this a=tag id=10\n", nowStr),
			lvl:  "error",
			msg:  "this",
		},
		{
			name: "simple double grouped field",
			exp:  fmt.Sprintf("ts=%s lvl=error msg=test a=tag id=10 test_a=this test_b=other\n", nowStr),
			lvl:  "error",
			msg:  "test",
			fields: []log.Field{
				log.GroupedFields("test", []log.Field{
					log.String("a", "this"),
					log.String("b", "other"),
				}),
			},
		},
		{
			name: "simple double strings field",
			exp:  fmt.Sprintf("ts=%s lvl=error msg=test a=tag id=10 test_0=this test_1=other\n", nowStr),
			lvl:  "error",
			msg:  "test",
			fields: []log.Field{
				log.Strings("test", []string{"this", "other"}),
			},
		},
		{
			name: "two fields",
			exp:  fmt.Sprintf("ts=%s lvl=error msg=test a=tag id=10 testing=\"that this\" works=1s\n", nowStr),
			lvl:  "error",
			msg:  "test",
			fields: []log.Field{
				log.String("testing", "that this"),
				log.Duration("works", time.Second),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			defer buf.Reset()
			l.Log(now, test.lvl, test.msg, test.fields)
			if exp, got := test.exp, buf.String(); exp != got {
				t.Fatalf("bad log line:\nexp: `%v`\ngot: `%v`", strconv.Quote(exp), strconv.Quote(got))
			}
		})
	}
}

// TODO: is there something better than this?
func TestLogger_SetLeveF(t *testing.T) {
	var logLine string
	buf := bytes.NewBuffer(nil)
	l := log.NewLogger(buf)
	msg := "the message"

	l.SetLevelF(func(lvl log.Level) bool {
		return lvl >= log.DebugLevel
	})
	l.Debug(msg)
	logLine = buf.String()
	buf.Reset()
	if logLine == "" {
		t.Fatal("expected debug log")
		return
	}
	l.Info(msg)
	logLine = buf.String()
	buf.Reset()
	if logLine == "" {
		t.Fatal("expected info log")
		return
	}
	buf.Reset()
	l.Warn(msg)
	logLine = buf.String()
	if logLine == "" {
		t.Fatal("expected warn log")
		return
	}
	l.Error(msg)
	logLine = buf.String()
	buf.Reset()
	if logLine == "" {
		t.Fatal("expected error log")
		return
	}

	l.SetLevelF(func(lvl log.Level) bool {
		return lvl >= log.InfoLevel
	})
	l.Debug(msg)
	logLine = buf.String()
	buf.Reset()
	if logLine != "" {
		t.Fatal("expected no debug log")
		return
	}
	l.Info(msg)
	logLine = buf.String()
	buf.Reset()
	if logLine == "" {
		t.Fatal("expected info log")
		return
	}
	buf.Reset()
	l.Warn(msg)
	logLine = buf.String()
	if logLine == "" {
		t.Fatal("expected warn log")
		return
	}
	l.Error(msg)
	logLine = buf.String()
	buf.Reset()
	if logLine == "" {
		t.Fatal("expected error log")
		return
	}

	l.SetLevelF(func(lvl log.Level) bool {
		return lvl >= log.WarnLevel
	})
	l.Debug(msg)
	logLine = buf.String()
	buf.Reset()
	if logLine != "" {
		t.Fatal("expected no debug log")
		return
	}
	l.Info(msg)
	logLine = buf.String()
	buf.Reset()
	if logLine != "" {
		t.Fatal("expected no info log")
		return
	}
	buf.Reset()
	l.Warn(msg)
	logLine = buf.String()
	if logLine == "" {
		t.Fatal("expected warn log")
		return
	}
	l.Error(msg)
	logLine = buf.String()
	buf.Reset()
	if logLine == "" {
		t.Fatal("expected error log")
		return
	}

	l.SetLevelF(func(lvl log.Level) bool {
		return lvl >= log.ErrorLevel
	})
	l.Debug(msg)
	logLine = buf.String()
	buf.Reset()
	if logLine != "" {
		t.Fatal("expected no debug log")
		return
	}
	l.Info(msg)
	logLine = buf.String()
	buf.Reset()
	if logLine != "" {
		t.Fatal("expected no info log")
		return
	}
	buf.Reset()
	l.Warn(msg)
	logLine = buf.String()
	if logLine != "" {
		t.Fatal("expected no warn log")
		return
	}
	l.Error(msg)
	logLine = buf.String()
	buf.Reset()
	if logLine == "" {
		t.Fatal("expected error log")
		return
	}
}
