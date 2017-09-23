package bufpool

import (
	"bytes"
	"io"
	"sync"
)

type Pool struct {
	p *sync.Pool
}

func New() *Pool {
	syncPool := sync.Pool{}
	syncPool.New = func() interface{} {
		return &closingBuffer{
			pool: &syncPool,
		}
	}

	return &Pool{
		p: &syncPool,
	}
}

func (p *Pool) Get() *closingBuffer {
	return p.p.Get().(*closingBuffer)
}

type closingBuffer struct {
	bytes.Buffer
	io.Closer
	pool *sync.Pool
}

func (cb *closingBuffer) Close() error {
	cb.Reset()
	cb.pool.Put(cb)
	return nil
}
