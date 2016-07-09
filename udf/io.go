package udf

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/golang/protobuf/proto"
)

//go:generate protoc --go_out=./ --python_out=./agent/py/kapacitor/udf/ udf.proto

// Interface for reading messages
// If you have an io.Reader
// wrap your reader in a bufio Reader
// to stasify this interface.
//
// Example:
// brr := bufio.NewReader(reader)
type ByteReadReader interface {
	io.Reader
	io.ByteReader
}

// Write the message to the io.Writer with a varint size header.
func WriteMessage(msg proto.Message, w io.Writer) error {
	// marshal message
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	varint := make([]byte, binary.MaxVarintLen32)
	n := binary.PutUvarint(varint, uint64(len(data)))

	_, err = w.Write(varint[:n])
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	if err != nil {
		return err
	}
	return nil
}

// Read a message from io.ByteReader by first reading a varint size,
// and then reading and decoding the message object.
// If buf is not big enough a new buffer will be allocated to replace buf.
func ReadMessage(buf *[]byte, r ByteReadReader, msg proto.Message) error {
	size, err := binary.ReadUvarint(r)
	if err != nil {
		return err
	}
	if cap(*buf) < int(size) {
		*buf = make([]byte, size)
	}
	b := (*buf)[:size]
	read := uint64(0)

	for read != size {
		n, err := r.Read(b[read:])
		if err == io.EOF {
			return fmt.Errorf("unexpected EOF, expected %d more bytes", size)
		}
		if err != nil {
			return err
		}
		read += uint64(n)
	}
	err = proto.Unmarshal(b, msg)
	if err != nil {
		return err
	}
	return nil
}
