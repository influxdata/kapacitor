package uuid

/****************
 * Date: 14/02/14
 * Time: 9:08 PM
 ***************/

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestGenerator_NewV1(t *testing.T) {
	u := NewV1()

	assert.Equal(t, 1, u.Version(), "Expected correct version")
	assert.Equal(t, ReservedRFC4122, u.Variant(), "Expected correct variant")
	assert.True(t, parseUUIDRegex.MatchString(u.String()), "Expected string representation to be valid")
}

func TestGenerator_NewV2(t *testing.T) {
	u := NewV2(DomainGroup)

	assert.Equal(t, 2, u.Version(), "Expected correct version")
	assert.Equal(t, ReservedRFC4122, u.Variant(), "Expected correct variant")
	assert.True(t, parseUUIDRegex.MatchString(u.String()), "Expected string representation to be valid")
	assert.Equal(t, uint8(DomainGroup), u.Bytes()[9], "Expected string representation to be valid")

	u = NewV2(DomainPerson)

	assert.Equal(t, 2, u.Version(), "Expected correct version")
	assert.Equal(t, ReservedRFC4122, u.Variant(), "Expected correct variant")
	assert.True(t, parseUUIDRegex.MatchString(u.String()), "Expected string representation to be valid")
	assert.Equal(t, uint8(DomainPerson), u.Bytes()[9], "Expected string representation to be valid")
}

type save struct {
	saved bool
	store *Store
	err   error
}

func (o *save) Save(pStore *Store) {
	o.saved = true
}

func (o *save) Read() (error, Store) {
	if o.store != nil {
		return nil, *o.store
	}
	if o.err != nil {
		return o.err, Store{}
	}
	return nil, Store{}
}

func TestRegisterSaver(t *testing.T) {
	registerTestGenerator(Timestamp(2048), []byte{0xaa})

	saver := &save{store: &Store{}}
	RegisterSaver(saver)

	assert.NotNil(t, generator.Saver, "Saver should save")
	registerDefaultGenerator()
}

func TestSaverRead(t *testing.T) {
	now, node := registerTestGenerator(Now().Sub(time.Second), []byte{0xaa})

	storageStamp := registerSaver(now.Sub(time.Second*2), node)

	assert.NotNil(t, generator.Saver, "Saver should save")
	assert.NotNil(t, generator.Store, "Default generator store should not return an empty store")
	assert.Equal(t, Sequence(2), generator.Store.Sequence, "Successfull read should have actual given sequence")
	assert.True(t, generator.Store.Timestamp > storageStamp, "Failed read should generate a time")
	assert.NotEmpty(t, generator.Store.Node, "There should be a node id")

	// Read returns an error
	_, node = registerTestGenerator(Now(), []byte{0xaa})
	saver := &save{err: errors.New("Read broken")}
	RegisterSaver(saver)

	assert.Nil(t, generator.Saver, "Saver should not exist")
	assert.NotNil(t, generator.Store, "Default generator store should not return an empty store")
	assert.NotEqual(t, Sequence(0), generator.Sequence, "Failed read should generate a non zero random sequence")
	assert.True(t, generator.Timestamp > 0, "Failed read should generate a time")
	assert.Equal(t, node, generator.Node, "There should be a node id")
	registerDefaultGenerator()
}

func TestSaverSave(t *testing.T) {
	registerTestGenerator(Now().Add(1024), []byte{0xdd, 0xee, 0xff, 0xaa, 0xbb})

	saver := &save{}
	RegisterSaver(saver)

	NewV1()
	time.Sleep(time.Second)

	assert.True(t, saver.saved, "Saver should save")
	registerDefaultGenerator()
}

func TestGeneratorInit(t *testing.T) {
	// A new time that is older than stored time should cause the sequence to increment
	now, node := registerTestGenerator(Now(), []byte{0xdd, 0xee, 0xff, 0xaa, 0xbb})
	storageStamp := registerSaver(now.Add(time.Second), node)

	assert.NotNil(t, generator.Store, "Generator should not return an empty store")
	assert.True(t, generator.Timestamp < storageStamp, "Increment sequence when old timestamp newer than new")
	assert.Equal(t, Sequence(3), generator.Sequence, "Successfull read should have incremented sequence")

	// Nodes not the same should generate a random sequence
	now, node = registerTestGenerator(Now(), []byte{0xdd, 0xee, 0xff, 0xaa, 0xbb})
	storageStamp = registerSaver(now.Sub(time.Second), []byte{0xaa, 0xee, 0xaa, 0xbb})

	assert.NotNil(t, generator.Store, "Generator should not return an empty store")
	assert.True(t, generator.Timestamp > storageStamp, "New timestamp should be newer than old")
	assert.NotEqual(t, Sequence(2), generator.Sequence, "Sequence should not be same as storage")
	assert.NotEqual(t, Sequence(3), generator.Sequence, "Sequence should not be incremented but be random")
	assert.Equal(t, generator.Node, node, generator.Sequence, "Node should be equal")

	registerDefaultGenerator()
}

func TestGeneratorRead(t *testing.T) {
	// A new time that is older than stored time should cause the sequence to increment
	now := Now()
	i := 0

	timestamps := []Timestamp{
		now.Sub(time.Second),
		now.Sub(time.Second * 2),
	}

	generator = newGenerator(
		func() Timestamp {
			return timestamps[i]
		},
		func() Node {
			return []byte{0xdd, 0xee, 0xff, 0xaa, 0xbb}
		},
		CleanHyphen)

	storageStamp := registerSaver(now.Add(time.Second), []byte{0xdd, 0xee, 0xff, 0xaa, 0xbb})

	i++

	store := generator.read()

	assert.True(t, store.Sequence != 0, "Should not return an empty store")
	assert.True(t, store.Timestamp != 0, "Should not return an empty store")
	assert.NotEmpty(t, store.Node, "Should not return an empty store")

	assert.True(t, store.Timestamp < storageStamp, "Increment sequence when old timestamp newer than new")
	assert.Equal(t, Sequence(4), store.Sequence, "Successfull read should have incremented sequence")

	// A new time that is older than stored time should cause the sequence to increment
	now, node := registerTestGenerator(Now().Sub(time.Second), []byte{0xdd, 0xee, 0xff, 0xaa, 0xbb})
	storageStamp = registerSaver(now.Add(time.Second), node)

	store = generator.read()

	assert.NotEqual(t, 0, store.Sequence, "Should return an empty store")
	assert.NotEmpty(t, store.Node, "Should not return an empty store")

	// A new time that is older than stored time should cause the sequence to increment
	registerTestGenerator(Now().Sub(time.Second), nil)
	storageStamp = registerSaver(now.Add(time.Second), []byte{0xdd, 0xee, 0xff, 0xaa, 0xbb})

	store = generator.read()
	assert.NotEmpty(t, store.Node, "Should not return an empty store")
	assert.NotEqual(t, []byte{0xdd, 0xee, 0xff, 0xaa, 0xbb}, store.Node, "Should not return an empty store")

	registerDefaultGenerator()

}

func TestGeneratorSave(t *testing.T) {
	registerTestGenerator(Now(), []byte{0xdd, 0xee, 0xff, 0xaa, 0xbb})
	generator.save()
	registerDefaultGenerator()
}

func TestStore_String(t *testing.T) {
	store := &Store{Node: []byte{0xdd, 0xee, 0xff, 0xaa, 0xbb}, Sequence: 2, Timestamp: 3}
	assert.Equal(t, "Timestamp[2167-05-04 23:34:33.709551916 +0000 UTC]-Sequence[2]-Node[ddeeffaabb]", store.String(), "The output store string should match")
}

func TestGetHardwareAddress(t *testing.T) {
	addr := findFirstHardwareAddress()
	assert.NotEmpty(t, addr, "There should be a node id")
}

func registerTestGenerator(pNow Timestamp, pId Node) (Timestamp, Node) {
	generator = newGenerator(
		func() Timestamp {
			return pNow
		},
		func() Node {
			return pId
		},
		CleanHyphen)
	return pNow, pId
}

func registerSaver(pStorageStamp Timestamp, pNode Node) (storageStamp Timestamp) {
	storageStamp = pStorageStamp

	saver := &save{store: &Store{Node: pNode, Sequence: 2, Timestamp: pStorageStamp}}
	RegisterSaver(saver)
	return
}
