package uuid

/****************
 * Date: 14/02/14
 * Time: 7:43 PM
 ***************/

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
)

// ****************************************************

var (
	posixUID = uint32(os.Getuid())
	posixGID = uint32(os.Getgid())
)

// ****************************************************

// Run this method before any calls to NewV1 or NewV2 to save the state to
// You must implement the uuid.Saver interface and are completely responsible
// for the non violable storage of the state
func RegisterSaver(pSaver Saver) {
	generator.Do(func() {
		defer generator.init()
		generator.Lock()
		defer generator.Unlock()
		generator.Saver = pSaver
	})
}

// Use this interface to setup a non volatile store within your system
// if you wish to have  v1 and 2 UUIDs based on your node id and constant time
// it is highly recommended to implement this
// You could use FileSystemStorage, the default is to generate random sequences
type Saver interface {
	// Read is run once, use this to setup your UUID state machine
	// Read should also return the UUID state from the non volatile store
	Read() (error, Store)

	// Save saves the state to the non volatile store and is called only if
	Save(*Store)
}

// The storage data to ensure continuous running of the UUID generator between restarts
type Store struct {
	// the last time UUID was saved
	Timestamp

	// an iterated value to help ensure different
	// values across the same domain
	Sequence

	// the last node which saved a UUID
	Node
}

func (o Store) String() string {
	return fmt.Sprintf("Timestamp[%s]-Sequence[%d]-Node[%x]", o.Timestamp, o.Sequence, o.Node)
}

type Generator struct {
	sync.Mutex
	sync.Once

	Saver
	*Store

	Next func() Timestamp
	Id   func() Node

	Fmt string
}

// NewV1 generates a new RFC4122 version 1 UUID
// based on a 60 bit timestamp and node id
func (o *Generator) NewV1() UUID {
	store := o.read()

	id := new(uuid)
	id.timeLow = uint32(store.Timestamp)
	return makeUuid(id, store, uint8(store.Sequence), 1)
}

// NewV2 generates a new DCE version 2 UUID
// based on a 60 bit timestamp, node id and POSIX UID or GUID
func (o *Generator) NewV2(pDomain DCEDomain) UUID {
	store := o.read()

	id := new(uuid)

	switch pDomain {
	case DomainPerson:
		id.timeLow = posixUID
	case DomainGroup:
		id.timeLow = posixGID
	}

	return makeUuid(id, store, uint8(pDomain), 2)
}

func makeUuid(pId *uuid, pStore *Store, pSequenceLow uint8, pVersion uint16) UUID {

	pId.timeMid = uint16(pStore.Timestamp >> 32)
	pId.timeHiAndVersion = uint16((pStore.Timestamp >> 48) & 0x0fff)
	pId.timeHiAndVersion |= (pVersion << 12)

	pId.sequenceHiAndVariant = uint8((pStore.Sequence & 0x3f00) >> 8)
	pId.sequenceHiAndVariant |= ReservedRFC4122

	pId.sequenceLow = pSequenceLow

	pId.node = make([]byte, 6)

	copy(pId.node[:], pStore.Node[:])
	pId.size = length

	return pId
}

func (o *Generator) read() *Store {

	// From a system-wide shared stable store (e.g., a file), read the
	// UUID generator state: the values of the timestamp, clock sequence,
	// and node ID used to generate the last UUID.
	o.Do(o.init)

	// Save the state (current timestamp, clock sequence, and node ID)
	// back to the stable store
	defer o.save()

	// Obtain a lock
	o.Lock()
	defer o.Unlock()

	// Get the current time as a 60-bit count of 100-nanosecond intervals
	// since 00:00:00.00, 15 October 1582.
	now := o.Next()

	// If the last timestamp is later than
	// the current timestamp, increment the clock sequence value.
	if now < o.Timestamp {
		o.Sequence++
	}

	// Update the timestamp
	o.Timestamp = now

	return o.Store
}

func (o *Generator) init() {
	// From a system-wide shared stable store (e.g., a file), read the
	// UUID generator state: the values of the timestamp, clock sequence,
	// and node ID used to generate the last UUID.
	var (
		storage Store
		err     error
	)

	// Save the state (current timestamp, clock sequence, and node ID)
	// back to the stable store.
	defer o.save()

	o.Lock()
	defer o.Unlock()

	if o.Saver != nil {
		err, storage = o.Read()
		if err != nil {
			o.Saver = nil
		}
	}

	// Get the current time as a 60-bit count of 100-nanosecond intervals
	// since 00:00:00.00, 15 October 1582.
	now := o.Next()

	//  Get the current node id
	node := o.Id()

	if node == nil {
		log.Println("uuid.Generator.init: address error: will generate random node id instead", err)

		node = make([]byte, 6)
		rand.Read(node)
		// Mark as randomly generated
		node[0] |= 0x01
	}

	// If the state was unavailable (e.g., non-existent or corrupted), or
	// the saved node ID is different than the current node ID, generate
	// a random clock sequence value.
	if o.Saver == nil || !bytes.Equal(storage.Node, node) {

		// 4.1.5.  Clock Sequence https://www.ietf.org/rfc/rfc4122.txt
		//
		// For UUID version 1, the clock sequence is used to help avoid
		// duplicates that could arise when the clock is set backwards in time
		// or if the node ID changes.
		//
		// If the clock is set backwards, or might have been set backwards
		// (e.g., while the system was powered off), and the UUID generator can
		// not be sure that no UUIDs were generated with timestamps larger than
		// the value to which the clock was set, then the clock sequence has to
		// be changed.  If the previous value of the clock sequence is known, it
		// can just be incremented; otherwise it should be set to a random or
		// high-quality pseudo-random value.

		// The clock sequence MUST be originally (i.e., once in the lifetime of
		// a system) initialized to a random number to minimize the correlation
		// across systems.  This provides maximum protection against node
		// identifiers that may move or switch from system to system rapidly.
		// The initial value MUST NOT be correlated to the node identifier.
		binary.Read(rand.Reader, binary.BigEndian, &storage.Sequence)
		log.Printf("uuid.Generator.init initialised random sequence: [%d]", storage.Sequence)

		// If the state was available, but the saved timestamp is later than
		// the current timestamp, increment the clock sequence value.

	} else if now < storage.Timestamp {
		storage.Sequence++
	}

	storage.Timestamp = now
	storage.Node = node

	o.Store = &storage
}

func (o *Generator) save() {
	if o.Saver != nil {
		go func(pState *Generator) {
			pState.Lock()
			defer pState.Unlock()
			pState.Save(pState.Store)
		}(o)
	}
}

func findFirstHardwareAddress() (node Node) {
	interfaces, err := net.Interfaces()
	if err == nil {
		for _, i := range interfaces {
			if i.Flags&net.FlagUp != 0 && bytes.Compare(i.HardwareAddr, nil) != 0 {
				// Don't use random as we have a real address
				node = Node(i.HardwareAddr)
				log.Println("uuid.getHardwareAddress:", node)
				break
			}
		}
	}
	return
}
