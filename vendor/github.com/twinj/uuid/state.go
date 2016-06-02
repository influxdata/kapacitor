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

// **************************************************** State

var (
	posixUID = uint32(os.Getuid())
	posixGID = uint32(os.Getgid())
)

// Run this method before any calls to NewV1 or NewV2 to save the state to
// YOu must implement the uuid.Saver interface and are completely responsible
// for the non violable storage of the state
func SetupSaver(pStateStorage Saver) {
	generator.Do(func() {
		defer generator.init()
		generator.Lock()
		defer generator.Unlock()
		generator.Saver = pStateStorage
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
	return fmt.Sprint(o.Timestamp, o.Sequence, o.Node)
}

type Generator struct {
	sync.Mutex
	sync.Once

	Saver
	*Store

	next func() Timestamp
	node func() Node

	format string
}

// Generate a new RFC4122 version 1 UUID
// based on a 60 bit timestamp and node id
func (o *Generator) NewV1() UUID {
	store := o.read()

	id := new(uuid)

	id.timeLow = uint32(store.Timestamp & 0xffffffff)
	id.timeMid = uint16((store.Timestamp >> 32) & 0xffff)
	id.timeHiAndVersion = uint16((store.Timestamp >> 48) & 0x0fff)
	id.timeHiAndVersion |= uint16(1 << 12)
	id.sequenceLow = byte(store.Sequence & 0xff)
	id.sequenceHiAndVariant = byte((store.Sequence & 0x3f00) >> 8)
	id.sequenceHiAndVariant |= ReservedRFC4122

	id.node = make([]byte, len(store.Node))

	copy(id.node[:], store.Node)
	id.size = length

	return id
}

// Generate a new DCE version 2 UUID
// based on a 60 bit timestamp and node id
func (o *Generator) NewV2() UUID {
	//store := o.read()

	id := new(uuid)

	//switch pDomain {
	//	case DomainPerson:
	//		binary.BigEndian.PutUint32(u[0:], posixUID)
	//	case DomainGroup:
	//	binary.BigEndian.PutUint32(u[0:], posixGID)
	//}
	//
	//o.timeLow = uint32(now & 0xffffffff)
	//o.timeMid = uint16((now >> 32) & 0xffff)
	//o.timeHiAndVersion = uint16((now >> 48) & 0x0fff)
	//o.timeHiAndVersion |= uint16(1 << 12)
	//o.sequenceLow = byte(sequence & 0xff)
	//o.sequenceHiAndVariant = byte((sequence & 0x3f00) >> 8)
	//o.sequenceHiAndVariant |= ReservedRFC4122
	//o.node = make([]byte, len(node))
	//copy(o.node[:], node)
	//o.size = length

	return id
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
	now := o.next()

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
	now := o.next()

	//  Get the current node ID.
	node := o.node()

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
		// TODO write test for when random
		err := binary.Read(rand.Reader, binary.LittleEndian, &storage.Sequence)
		if err != nil {
			log.Println("uuid.Generator.init error:", err)
		} else {
			log.Printf("uuid.Generator.init initialised random sequence: [%d]", storage.Sequence)
		}

		// If the state was available, but the saved timestamp is later than
		// the current timestamp, increment the clock sequence value.

	} else if now < storage.Timestamp {
		storage.Sequence++
	}

	storage.Timestamp = now;
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

func getHardwareAddress() (node Node) {
	interfaces, err := net.Interfaces()
	if err == nil {
		for _, i := range interfaces {
			// Initially I could multi-cast out the Flags to get
			// whether the interface was up but started failing
			if (i.Flags & (1 << net.FlagUp)) != 0 {
				//if inter.Flags.String() != "0" {
				if addrs, err := i.Addrs(); err == nil {
					for _, a := range addrs {
						if a.String() != "0.0.0.0" && !bytes.Equal(i.HardwareAddr, make([]byte, len(i.HardwareAddr))) {
							// Don't use random as we have a real address
							node = Node(i.HardwareAddr)
							log.Println("uuid.getHardwareAddress:", node)

							return
						}
					}
				}
			}
		}
	}
	log.Println("uuid.getHardwareAddress: address error: will generate random node id instead", err)

	// TODO write test for when random
	node = make([]byte, 6)

	if _, err := rand.Read(node); err != nil {
		log.Panicln("uuid.getHardwareAddress: could not get cryto random bytes", err)
	}

	log.Println("uuid.getHardwareAddress: generated node", node)

	// Mark as randomly generated
	node[0] |= 0x01
	return
}
