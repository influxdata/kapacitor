package kafkatest

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
)

// Provides an incomplete Kafka Server implementation.
// Records messages sent via the ProduceRequest and responds to MetadataRequests as the only broker.
type Server struct {
	Addr net.Addr
	mu   sync.Mutex
	wg   sync.WaitGroup

	closed  bool
	closing chan struct{}

	messages []Message
	errors   []error

	brokerMessage []byte
	nodeID        int32

	partitionMessage []byte
}

func NewServer() (*Server, error) {
	s := &Server{
		closing: make(chan struct{}),
		nodeID:  1,
	}
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		return nil, err
	}
	s.Addr = l.Addr()

	// Prepare static message bytes
	s.prepareBrokerMsg()
	s.preparePartitionMsg()

	// start server
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.run(l)
	}()
	return s, nil
}

func (s *Server) prepareBrokerMsg() {
	host, port, _ := net.SplitHostPort(s.Addr.String())
	s.brokerMessage = make([]byte, 0, 4+2+len(host)+4)
	s.brokerMessage = writeInt32(s.brokerMessage, s.nodeID)
	s.brokerMessage = writeStr(s.brokerMessage, host)
	portN, _ := strconv.Atoi(port)
	s.brokerMessage = writeInt32(s.brokerMessage, int32(portN))
}

func (s *Server) preparePartitionMsg() {
	s.partitionMessage = make([]byte, 0, 2+4+4+4+4)
	// Write error code
	s.partitionMessage = writeInt16(s.partitionMessage, 0)
	// Write partition ID
	s.partitionMessage = writeInt32(s.partitionMessage, 1)
	// Write leader ID
	s.partitionMessage = writeInt32(s.partitionMessage, s.nodeID)
	// Write 0 len replicas
	s.partitionMessage = writeArrayHeader(s.partitionMessage, 0)
	// Write 0 len Isr
	s.partitionMessage = writeArrayHeader(s.partitionMessage, 0)
}

func (s *Server) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	close(s.closing)
	s.wg.Wait()
}

func (s *Server) Messages() ([]Message, error) {
	if len(s.errors) > 0 {
		return nil, multiError(s.errors)
	}
	return s.messages, nil
}

func (s *Server) run(l net.Listener) {
	defer l.Close()

	accepts := make(chan net.Conn)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			select {
			case accepts <- c:
			case <-s.closing:
				return
			}
		}
	}()

	for {
		select {
		case c := <-accepts:
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				defer c.Close()
				if err := s.handle(c); err != nil {
					s.errors = append(s.errors, err)
				}
			}()
		case <-s.closing:
			return
		}
	}
}

func (s *Server) handle(c net.Conn) error {
	var size int32
	err := binary.Read(c, binary.BigEndian, &size)
	if err != nil {
		return err
	}
	buf := make([]byte, int(size))
	io.ReadFull(c, buf)
	if err != nil {
		return err
	}
	// ApiKey indicated the type of request
	apiKey := int16(binary.BigEndian.Uint16(buf[:2]))
	_, n := readStr(buf[8:])
	request := buf[8+n:]

	// Prepare response
	response := make([]byte, 8, 1024)
	// Leave first 4 bytes for the size.
	// Copy correlationID.
	copy(response[4:], buf[4:8])

	switch apiKey {
	case 0: // ProduceRequest
		topic, partition, offset := s.readProduceRequest(request)

		// Prepare success response
		response = writeArrayHeader(response, 1)
		response = writeStr(response, topic)
		response = writeArrayHeader(response, 1)
		response = writeInt32(response, partition)
		response = writeInt16(response, 0) // Error Code
		response = writeInt64(response, offset)
		response = writeInt64(response, -1) // Timestamp
		response = writeInt32(response, 0)  // ThrottleTime
	case 3: // Metadata
		topics, _ := readStrList(request)

		// Write broker message
		response = writeArray(response, [][]byte{s.brokerMessage})

		// Write topic metadata
		response = writeArrayHeader(response, int32(len(topics)))
		for _, t := range topics {
			// Write Error Code
			response = writeInt16(response, 0)
			// Write topic name
			response = writeStr(response, t)

			// Write partition
			response = writeArray(response, [][]byte{s.partitionMessage})
		}
	default:
		return fmt.Errorf("unsupported apiKey %d", apiKey)
	}

	// Set response size
	responseSize := len(response) - 4
	binary.BigEndian.PutUint32(response[:4], uint32(responseSize))
	_, err = c.Write(response)
	return err
}

// readProduceRequest, assume only a single message exists
func (s *Server) readProduceRequest(request []byte) (topic string, partition int32, offset int64) {
	pos := 2 + 4 + 4 // skip RequiredAcks and Timeout and array len

	// Read topic name
	topic, n := readStr(request[pos:])
	pos += n

	pos += 4 // skip array len

	partition = readInt32(request[pos:])
	pos += 4

	pos += 4 // skip set size

	offset = readInt64(request[pos:])
	pos += 8

	pos += 4 + 4 + 1 + 1 + 8 // skip size, crc, magic, attributes, timestamp

	key, n := readByteArray(request[pos:])
	pos += n

	message, n := readByteArray(request[pos:])
	pos += n

	s.saveMessage(Message{
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		Key:       string(key),
		Message:   string(message),
	})
	return
}

func (s *Server) saveMessage(m Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages = append(s.messages, m)
}

func readList(buf []byte, f func([]byte) int) int {
	pos := 4
	count := int(int32(binary.BigEndian.Uint32(buf[:pos])))
	for i := 0; i < count; i++ {
		pos += f(buf[pos:])
	}
	return pos
}
func readStrList(buf []byte) ([]string, int) {
	var strs []string
	l := readList(buf, func(data []byte) int {
		s, n := readStr(data)
		strs = append(strs, s)
		return n
	})
	return strs, l
}
func readStr(buf []byte) (string, int) {
	n := int(int16(binary.BigEndian.Uint16(buf[:2])))
	return string(buf[2 : 2+n]), n + 2
}
func readByteArray(buf []byte) ([]byte, int) {
	n := int(int32(binary.BigEndian.Uint32(buf[:4])))
	return buf[4 : 4+n], n + 4
}

func readInt16(buf []byte) int16 {
	return int16(binary.BigEndian.Uint16(buf[:2]))
}
func readInt32(buf []byte) int32 {
	return int32(binary.BigEndian.Uint32(buf[:4]))
}
func readInt64(buf []byte) int64 {
	return int64(binary.BigEndian.Uint64(buf[:8]))
}

func writeStr(dst []byte, s string) []byte {
	dst = writeInt16(dst, len(s))
	return append(dst, []byte(s)...)
}

func writeInt16(dst []byte, n int) []byte {
	l := len(dst)
	dst = append(dst, []byte{0, 0}...)
	binary.BigEndian.PutUint16(dst[l:l+2], uint16(n))
	return dst
}

func writeInt32(dst []byte, n int32) []byte {
	l := len(dst)
	dst = append(dst, []byte{0, 0, 0, 0}...)
	binary.BigEndian.PutUint32(dst[l:l+4], uint32(n))
	return dst
}
func writeInt64(dst []byte, n int64) []byte {
	l := len(dst)
	dst = append(dst, []byte{0, 0, 0, 0, 0, 0, 0, 0}...)
	binary.BigEndian.PutUint64(dst[l:l+8], uint64(n))
	return dst
}

func writeArrayHeader(dst []byte, n int32) []byte {
	return writeInt32(dst, n)
}

func writeArray(dst []byte, data [][]byte) []byte {
	dst = writeArrayHeader(dst, int32(len(data)))
	for _, d := range data {
		dst = append(dst, d...)
	}
	return dst
}

type multiError []error

func (e multiError) Error() string {
	errs := make([]string, len(e))
	for i := range e {
		errs[i] = e[i].Error()
	}
	return strings.Join(errs, "\n")
}

type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       string
	Message   string
}
