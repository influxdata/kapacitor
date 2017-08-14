package diagnostic

import (
	"bytes"
	"errors"
	"io"
	"strconv"
	"sync"
)

// fake example type
type pairEncoder struct {
	mu      sync.Mutex
	bufPool *sync.Pool

	w io.Writer
}

func NewPairEncoder(w io.Writer) *pairEncoder {
	p := &pairEncoder{
		bufPool: &sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
		w: w,
	}

	return p
}

func (p *pairEncoder) Encode(keyvalsList ...[]interface{}) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	buf := p.NewBuffer()
	// TODO: fomatting will be a bit off
	for _, keyvals := range keyvalsList {

		if len(keyvals)%2 != 0 {
			return errors.New("nope")
		}

		for i, el := range keyvals {
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

			if i+1 == len(keyvals) {
				continue
			}
			buf.WriteByte(' ')
		}
	}

	buf.WriteByte('\n')
	buf.WriteTo(p.w)

	p.Put(buf)

	return nil
}

func (p *pairEncoder) NewBuffer() *bytes.Buffer {
	buf := p.bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

func (p *pairEncoder) Put(b *bytes.Buffer) {
	p.bufPool.Put(b)
}
