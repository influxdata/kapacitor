package logging

import (
	"io"
	"sync"
	"time"

	"github.com/uber-go/zap"
	"github.com/uber-go/zap/zwrap"
)

// teeEncoder implements the zap.Encoder interface duplicating all actions to exactly two child encoders.
type teeEncoder struct {
	encoders [2]zap.Encoder
}

func newTeeEncoder(encoders [2]zap.Encoder) zap.Encoder {
	return teeEncoder{encoders: encoders}
}

func (t teeEncoder) AddBool(key string, value bool) {
	t.encoders[0].AddBool(key, value)
	t.encoders[1].AddBool(key, value)
}

func (t teeEncoder) AddFloat64(key string, value float64) {
	t.encoders[0].AddFloat64(key, value)
	t.encoders[1].AddFloat64(key, value)
}

func (t teeEncoder) AddInt(key string, value int) {
	t.encoders[0].AddInt(key, value)
	t.encoders[1].AddInt(key, value)
}

func (t teeEncoder) AddInt64(key string, value int64) {
	t.encoders[0].AddInt64(key, value)
	t.encoders[1].AddInt64(key, value)
}

func (t teeEncoder) AddUint(key string, value uint) {
	t.encoders[0].AddUint(key, value)
	t.encoders[1].AddUint(key, value)
}

func (t teeEncoder) AddUint64(key string, value uint64) {
	t.encoders[0].AddUint64(key, value)
	t.encoders[1].AddUint64(key, value)
}

func (t teeEncoder) AddUintptr(key string, value uintptr) {
	t.encoders[0].AddUintptr(key, value)
	t.encoders[1].AddUintptr(key, value)
}

func (t teeEncoder) AddMarshaler(key string, marshaler zap.LogMarshaler) error {
	if err := t.encoders[0].AddMarshaler(key, marshaler); err != nil {
		return err
	}
	if err := t.encoders[1].AddMarshaler(key, marshaler); err != nil {
		return err
	}
	return nil
}

func (t teeEncoder) AddObject(key string, value interface{}) error {
	if err := t.encoders[0].AddObject(key, value); err != nil {
		return err
	}
	if err := t.encoders[1].AddObject(key, value); err != nil {
		return err
	}
	return nil
}

func (t teeEncoder) AddString(key string, value string) {
	t.encoders[0].AddString(key, value)
	t.encoders[1].AddString(key, value)
}

func (t teeEncoder) Clone() zap.Encoder {
	c := teeEncoder{}
	c.encoders[0] = t.encoders[0].Clone()
	c.encoders[1] = t.encoders[1].Clone()
	return c
}

func (t teeEncoder) Free() {}

func (t teeEncoder) WriteEntry(w io.Writer, msg string, level zap.Level, et time.Time) error {
	if err := t.encoders[0].WriteEntry(w, msg, level, et); err != nil {
		return err
	}
	if err := t.encoders[1].WriteEntry(w, msg, level, et); err != nil {
		return err
	}
	return nil
}

type entry struct {
	Time    time.Time
	Message string
	Level   zap.Level
	Fields  zwrap.KeyValueMap
}

func (e entry) reset() {
	for k := range e.Fields {
		delete(e.Fields, k)
	}
}

var chanEncoderPool = &sync.Pool{
	New: func() interface{} {
		return &chanEncoder{
			e: entry{
				Fields: make(zwrap.KeyValueMap),
			},
		}
	},
}

// chanEncoder capatures all KeyValue data into an entry and sends the entry through a channel.
type chanEncoder struct {
	entries chan<- entry
	e       entry
}

func newChanEncoder(entries chan<- entry) zap.Encoder {
	return &chanEncoder{
		entries: entries,
		e: entry{
			Fields: make(zwrap.KeyValueMap),
		},
	}
}

func (c *chanEncoder) AddBool(key string, value bool) {
	c.e.Fields.AddBool(key, value)
}

func (c *chanEncoder) AddFloat64(key string, value float64) {
	c.e.Fields.AddFloat64(key, value)
}

func (c *chanEncoder) AddInt(key string, value int) {
	c.e.Fields.AddInt(key, value)
}

func (c *chanEncoder) AddInt64(key string, value int64) {
	c.e.Fields.AddInt64(key, value)
}

func (c *chanEncoder) AddUint(key string, value uint) {
	c.e.Fields.AddUint(key, value)
}

func (c *chanEncoder) AddUint64(key string, value uint64) {
	c.e.Fields.AddUint64(key, value)
}

func (c *chanEncoder) AddUintptr(key string, value uintptr) {
	c.e.Fields.AddUintptr(key, value)
}

func (c *chanEncoder) AddMarshaler(key string, marshaler zap.LogMarshaler) error {
	return c.e.Fields.AddMarshaler(key, marshaler)
}

func (c *chanEncoder) AddObject(key string, value interface{}) error {
	return c.e.Fields.AddObject(key, value)
}

func (c *chanEncoder) AddString(key string, value string) {
	c.e.Fields.AddString(key, value)
}

func (c *chanEncoder) Clone() zap.Encoder {
	n := chanEncoderPool.Get().(*chanEncoder)
	n.entries = c.entries
	n.e.reset()
	return n
}

func (c *chanEncoder) Free() {
	chanEncoderPool.Put(c)
}

func (c *chanEncoder) WriteEntry(w io.Writer, msg string, l zap.Level, t time.Time) error {
	c.e.Time = t
	c.e.Message = msg
	c.e.Level = l

	select {
	case c.entries <- c.e:
	default:
	}
	return nil
}
