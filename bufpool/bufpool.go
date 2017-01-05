package bufpool

import (
	"bytes"
	"sync"
)

type Pool struct {
	p sync.Pool
}

func New() *Pool {
	return &Pool{
		p: sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
	}
}

func (p *Pool) Get() *bytes.Buffer {
	return p.p.Get().(*bytes.Buffer)
}

func (p *Pool) Put(b *bytes.Buffer) {
	b.Reset()
	p.p.Put(b)
}
