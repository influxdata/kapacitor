package udf_test

import (
	"bufio"
	"io"
	"log"

	"github.com/influxdata/kapacitor/udf"
)

// IO implements a UDF process communication.
// Connect up to UDF server via In/Out pipes.
// Use Requests/Responses channels for reading
// and writing requests and responses for the UDF.
type IO struct {
	inr *io.PipeReader
	inw *io.PipeWriter

	outr *io.PipeReader
	brr  udf.ByteReadReader
	outw *io.PipeWriter

	// Requests sent to the UDF
	Requests chan *udf.Request
	// Responses from the UDF
	Responses chan *udf.Response
	// Any error that may have occurred
	ErrC chan error
}

func NewIO() *IO {
	inr, inw := io.Pipe()
	outr, outw := io.Pipe()
	brr := bufio.NewReader(outr)
	u := &IO{
		Requests:  make(chan *udf.Request),
		Responses: make(chan *udf.Response),
		ErrC:      make(chan error, 1),
		inr:       inr,
		inw:       inw,
		outr:      outr,
		brr:       brr,
		outw:      outw,
	}
	go u.run()
	return u
}

// Forcefully kill the command.
// This will likely cause a panic.
func (o *IO) Kill() {
	close(o.Requests)
	close(o.Responses)
	close(o.ErrC)
	o.inr.Close()
	o.inw.Close()
	o.outr.Close()
	o.outw.Close()
}

func (o *IO) readRequests() error {
	defer o.inr.Close()
	defer close(o.Requests)
	buf := bufio.NewReader(o.inr)
	var b []byte
	for {
		req := &udf.Request{}
		err := udf.ReadMessage(&b, buf, req)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		o.Requests <- req
	}
}

func (o *IO) writeResponses() error {
	defer o.outw.Close()
	for response := range o.Responses {
		udf.WriteMessage(response, o.outw)
	}
	return nil
}

func (o *IO) run() {
	readErrC := make(chan error, 1)
	writeErrC := make(chan error, 1)
	go func() {
		readErrC <- o.readRequests()
	}()
	go func() {
		writeErrC <- o.writeResponses()
	}()
	var readErr, writeErr error
	for readErrC != nil || writeErrC != nil {
		select {
		case readErr = <-readErrC:
			readErrC = nil
		case writeErr = <-writeErrC:
			writeErrC = nil
		}
	}

	if readErr != nil {
		o.ErrC <- readErr
	} else {
		o.ErrC <- writeErr
	}
}

func (o *IO) In() io.WriteCloser {
	return o.inw
}

func (o *IO) Out() udf.ByteReadReader {
	return o.brr
}

type UDF struct {
	*udf.Server
	uio    *IO
	logger *log.Logger
}

func New(uio *IO, l *log.Logger) *UDF {
	return &UDF{
		uio:    uio,
		logger: l,
	}
}

func (u *UDF) Open() error {
	u.Server = udf.NewServer(u.uio.Out(), u.uio.In(), u.logger, 0, nil, nil)
	return u.Server.Start()
}

func (u *UDF) Close() error {
	return u.Server.Stop()
}
