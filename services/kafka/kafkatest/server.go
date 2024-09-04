package kafkatest

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
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

	partitionCount int32
}

func NewServer() (*Server, error) {
	sarama.Logger = log.Default()
	s := &Server{
		closing:        make(chan struct{}),
		nodeID:         1,
		partitionCount: 3,
	}
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		return nil, err
	}
	s.Addr = l.Addr()

	// Prepare static message bytes
	s.prepareBrokerMsg()
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
	s.brokerMessage = writeInt16(s.brokerMessage, -1)
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
			go func() {
				defer c.Close()
				for {
					if err := s.handle(c); err != nil {

						if err == io.EOF {
							return
						}
						s.mu.Lock()
						s.errors = append(s.errors, err)
						s.mu.Unlock()

					}
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

	_, err = io.ReadFull(c, buf)
	if err != nil {
		return err
	}

	// ApiKey indicated the type of request
	apiKey := int16(binary.BigEndian.Uint16(buf[:2]))
	// Version indicates the protocol version
	version := int16(binary.BigEndian.Uint16(buf[2:4]))
	// skip correlation id
	_, n := readStr(buf[8:]) //ClientID
	request := buf[8+n:]

	// Prepare response
	response := make([]byte, 8, 1024)
	// Leave first 4 bytes for the size.
	// Copy correlationID.
	copy(response[4:], buf[4:8])

	switch apiKey {
	case 0: // ProduceRequest
		topic, responses, err := s.readProduceRequest(request)
		if err != nil {
			return err
		}

		// Prepare success response
		response = writeArrayHeader(response, 1)
		response = writeStr(response, topic)
		response = writeArrayHeader(response, int32(len(responses)))
		for _, r := range responses {
			response = writeInt32(response, 0) //index
			response = writeInt16(response, 0) // Error Code
			response = writeInt64(response, r.offset)
			response = writeInt64(response, 0) //log_append_time_ms
			if version >= 5 {
				response = writeInt64(response, 0) // log_start_offset
			}
		}
		response = writeInt32(response, 0) //throttle_time_ms

	case 3: // Metadata
		topics, _ := readStrList(request)
		response = writeInt32(response, 0) //throttle_time_ms
		// Write broker message
		response = writeArray(response, [][]byte{s.brokerMessage})

		// Write cluster_id
		response = writeInt16(response, -1)

		// Write controller id
		response = writeInt32(response, 0)

		// Write topic metadata
		response = writeArrayHeader(response, int32(len(topics)))
		for _, t := range topics {
			// Write Error Code
			response = writeInt16(response, 0)
			// Write topic name
			response = writeStr(response, t)
			// Write is_internal
			response = writeBool(response, false)

			// Write partitions
			response = writeArrayHeader(response, s.partitionCount)
			for i := int32(0); i < s.partitionCount; i++ {
				// Write error code
				response = writeInt16(response, 0)
				// Write partition ID
				response = writeInt32(response, i+1)
				// Write leader ID
				response = writeInt32(response, s.nodeID)
				if version >= 7 {
					// write Leader Epoch
					response = writeInt32(response, 0)
				}
				// Write 0 len replicas
				response = writeArrayHeader(response, 0)
				// Write 0 len Isr
				response = writeArrayHeader(response, 0)
				// Write offline replicas
				response = writeArrayHeader(response, 0)

			}
		}
	case 18:
		// Hard code the api versions we are implementing to ensure
		// the client knows which ones we plan on implementing.
		response = writeInt16(response, 0) // Error Code

		// Write api versions
		response = writeArrayHeader(response, 2)

		// Write produce request version
		response = writeInt16(response, 0)
		response = writeInt16(response, 2)
		response = writeInt16(response, 2)

		// Write metadata request version
		response = writeInt16(response, 3)
		response = writeInt16(response, 1)
		response = writeInt16(response, 1)
	default:
		return fmt.Errorf("unsupported apiKey %d", apiKey)
	}

	// Set response size
	responseSize := len(response) - 4
	binary.BigEndian.PutUint32(response[:4], uint32(responseSize))
	_, err = c.Write(response)
	return err
}

type produceResponse struct {
	partition int32
	offset    int64
}

// readProduceRequest, assume only a single message per partition exists
func (s *Server) readProduceRequest(request []byte) (string, []produceResponse, error) {
	buf := []produceResponse{}
	_, pos := readNullableStr(request) // transactional_id

	pos += 2 + 4 + 4 // skip RequiredAcks and Timeout and array len

	// Read topic name
	topic, n := readStr(request[pos:])
	pos += n

	// Read array len
	_ = readInt32(request[pos:])
	pos += 4

	// Read partition ID
	partition := readInt32(request[pos:])
	pos += 4

	pos += 4 // skip the length of the payload

	offset := readInt64(request[pos:])
	pos += 8

	pos += 4               // skip message size
	pos += 4               // skip leader epoc
	if request[pos] != 2 { // magic byte sanity check
		return "", nil, errors.New("bad message, magic byte is not 2")
	}
	pos++

	pos += 4 // skip CRC32
	pos += 2 // skip some control codes
	pos += 4 // skip last offset delta

	msecs := readInt64(request[pos:]) // first timestamp
	pos += 8

	pos += 8 // skip last timestamps
	pos += 8 // skip producer ID
	pos += 2 // skip poducer epoch
	pos += 4 // skip base Sequence

	arrayLen := readInt32(request[pos:])
	pos += 4

	_, n = binary.Varint(request[pos:]) // skip over the length of bytes of the message
	pos += n
	for i := int32(0); i < arrayLen; i++ {
		pos++ // skip attributes

		tsOffset, n := binary.Varint(request[pos:])
		pos += n

		offsetDelta, n := binary.Varint(request[pos:])
		pos += n
		keyL, n := binary.Varint(request[pos:])
		pos += n
		key := request[pos : pos+int(keyL)]
		pos += int(keyL)
		messageL, n := binary.Varint(request[pos:])
		pos += n
		message := request[pos : pos+int(messageL)]
		pos += int(messageL)
		s.saveMessage(Message{
			Topic:     topic,
			Partition: int32(partition),
			Offset:    int64(offset + offsetDelta),
			Key:       string(key),
			Message:   string(message),
			Time:      time.Unix((msecs+tsOffset)/1000, ((msecs+tsOffset)%1000)*1000000).UTC(),
		})
		buf = append(buf, produceResponse{int32(partition), int64(offset)})
	}

	return topic, buf, nil
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

func readInt32(buf []byte) int32 {
	return int32(binary.BigEndian.Uint32(buf[:4]))
}
func readInt64(buf []byte) int64 {
	return int64(binary.BigEndian.Uint64(buf[:8]))
}

func readNullableStr(buf []byte) (*string, int) {
	n := int(int16(binary.BigEndian.Uint16(buf[:2])))
	if n == -1 {
		return nil, 2
	}
	s := string(buf[2 : 2+n])
	return &s, n + 2

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

func writeBool(dst []byte, b bool) []byte {
	v := int8(0)
	if b {
		v = 1
	}
	return writeInt8(dst, v)
}
func writeInt8(dst []byte, n int8) []byte {
	return append(dst, byte(n))
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
	Time      time.Time
}
