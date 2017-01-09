package snmpgo

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net"
	"regexp"
	"strings"
	"sync"
	"time"
)

var random *rand.Rand
var randOnce sync.Once

func initRandom() {
	random = rand.New(rand.NewSource(time.Now().UnixNano()))
}

var reqMutex sync.Mutex

func genRequestId() int {
	randOnce.Do(initRandom)
	reqMutex.Lock()
	val := int(random.Int31())
	reqMutex.Unlock()
	return val
}

func genSalt32() int32 {
	randOnce.Do(initRandom)
	reqMutex.Lock()
	val := random.Int31()
	reqMutex.Unlock()
	return val
}

func genSalt64() int64 {
	randOnce.Do(initRandom)
	reqMutex.Lock()
	val := random.Int63()
	reqMutex.Unlock()
	return val
}

var mesId int = math.MaxInt32 - 1
var mesMutex sync.Mutex

func genMessageId() (id int) {
	randOnce.Do(initRandom)
	mesMutex.Lock()
	mesId++
	if mesId == math.MaxInt32 {
		reqMutex.Lock()
		mesId = int(random.Int31())
		reqMutex.Unlock()
	}
	id = mesId
	mesMutex.Unlock()
	return
}

func retry(retries int, f func() error) (err error) {
	for i := 0; i <= retries; i++ {
		err = f()
		switch e := err.(type) {
		case net.Error:
			if e.Timeout() {
				continue
			}
		case *notInTimeWindowError:
			err = e.error
			continue
		}
		return
	}
	return
}

func confirmedType(t PduType) bool {
	if t == GetRequest || t == GetNextRequest || t == SetRequest ||
		t == GetBulkRequest || t == InformRequest {
		return true
	}
	return false
}

func engineIdToBytes(engineId string) ([]byte, error) {
	b, err := hex.DecodeString(engineId)
	if l := len(b); err != nil || (l < 5 || l > 32) {
		return nil, &ArgumentError{
			Value:   engineId,
			Message: "EngineId must be a hexadecimal string and length is range 5..32",
		}
	}
	return b, nil
}

var hexPrefix *regexp.Regexp = regexp.MustCompile(`^0[xX]`)

func stripHexPrefix(s string) string {
	return hexPrefix.ReplaceAllString(s, "")
}

func toHexStr(a []byte, sep string) string {
	s := make([]string, len(a))
	for i, b := range a {
		s[i] = fmt.Sprintf("%02x", b)
	}
	return strings.Join(s, sep)
}

func escape(s interface{}) string {
	r, _ := json.Marshal(s)
	return string(r)
}

func xor(a, b []byte) []byte {
	c := make([]byte, len(a))
	for i := 0; i < len(a); i++ {
		c[i] = a[i] ^ b[i]
	}
	return c
}

func padding(b []byte, size int) []byte {
	pad := size - (len(b) % size)
	if pad > 0 {
		b = append(b, bytes.Repeat([]byte{0x00}, pad)...)
	}
	return b
}
