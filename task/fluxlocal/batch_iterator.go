package fluxlocal

import "github.com/influxdata/flux"

type batchiterator struct {
	batch
}

func NewBatchIterator(flux.ResultIterator)
