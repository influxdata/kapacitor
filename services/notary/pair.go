package notary

import (
	"bytes"
	"errors"
	"io"
	"strconv"
	"sync"
)

type pairLogger struct {
	mu      sync.Mutex
	bufPool *sync.Pool
	//context *context
	io.Writer

	keys []interface{}
}

func NewPairLogger(w io.Writer) *pairLogger {
	//func NewPairLogger(w io.Writer, c *context) *pairLogger {
	p := &pairLogger{
		bufPool: &sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
		Writer: w,
	}

	return p
}

func (p *pairLogger) NewBuffer() *bytes.Buffer {
	buf := p.bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

func (t *pairLogger) Info(kv ...interface{}) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(kv)%2 != 0 {
		return errors.New("nope")
	}
	buf := t.NewBuffer()
	buf.WriteString("level=info ")

	for i, el := range kv {
		if i%2 == 0 {
			buf.WriteString(el.(string))
			continue
		}

		buf.WriteByte('=')
		switch el.(type) {
		case string:
			buf.WriteString(el.(string))
		case int:
			buf.WriteString(strconv.Itoa(el.(int)))
		}

		if i+1 == len(kv) {
			continue
		}
		buf.WriteByte(' ')
	}
	buf.WriteByte('\n')

	buf.WriteTo(t.Writer)

	t.Put(buf)

	return nil
}

func (t *pairLogger) Put(b *bytes.Buffer) {
	t.bufPool.Put(b)
}

func (t *pairLogger) Debug(kv ...interface{}) error {
	return nil
}

//func (t *pairLogger) Error(kv ...interface{}) error {
//	return nil
//}
func (t *pairLogger) Other(kv ...interface{}) error {
	return nil
}

func (t *pairLogger) Error(kv ...interface{}) error {
	if len(kv)%2 != 0 {
		return errors.New("nope")
	}

	t.Write([]byte("level=erro"))

	for i, el := range kv {
		if i%2 == 0 {
			t.Write([]byte(el.(string)))
			continue
		}

		t.Write([]byte("="))
		switch el.(type) {
		case string:
			t.Write([]byte(el.(string)))
		case int:
			t.Write([]byte(strconv.Itoa(el.(int))))
		}

		if i+1 == len(kv) {
			continue
		}
		t.Write([]byte("="))
	}

	return nil
}
