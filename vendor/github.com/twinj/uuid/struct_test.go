package uuid

/****************
 * Date: 15/02/14
 * Time: 12:26 PM
 ***************/

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

var uuidId = &uuid{
	0xaacfee12,
	0xd400,
	0x2723,
	0x00,
	0xd3,
	uint8(length),
	[]byte{0x23, 0x12, 0x4A, 0x11, 0x89, 0xbb},
}

func TestUuid_Bytes(t *testing.T) {
	assert.Equal(t, uuidBytes[:], uuidId.Bytes(), "Bytes should be the same")
}

func TestUuid_Unmarshal(t *testing.T) {
	id := &uuid{size: length, node: make([]byte, 6)}
	id.Unmarshal(uuidBytes[:])

	assert.Equal(t, uuidId.String(), id.String(), "String should be the same")
}

func TestUuid_MarshalBinary(t *testing.T) {
	bytes, err := uuidId.MarshalBinary()
	assert.Nil(t, err, "There should be no error")
	assert.Equal(t, uuidBytes[:], bytes, "Byte should be the same")
}

func TestUuid_Size(t *testing.T) {
	assert.Equal(t, 16, uuidId.Size(), "The size of the uuid should be sixteen")
}

func TestUuid_String(t *testing.T) {
	assert.Equal(t, idString, uuidId.String(), "The Format given should match the output")
}

func TestUuid_UnmarshalBinary(t *testing.T) {

	u := &uuid{size: 16, node: make([]byte, 6)}

	err := u.UnmarshalBinary([]byte{1, 2, 3, 4, 5})

	assert.Equal(t, "uuid.UnmarshalBinary: invalid length", err.Error(), "Expect length error")

	err = u.UnmarshalBinary(uuidBytes[:])

	assert.Nil(t, err, "There should be no error but got %s", err)

	for k, v := range namespaces {
		id, _ := Parse(v)
		newId := &uuid{size: 16, node: make([]byte, 6)}
		newId.UnmarshalBinary(id.Bytes())

		assert.Equal(t, id.Bytes(), newId.Bytes(), "The array id should equal the uuid id")
		assert.Equal(t, k.Bytes(), newId.Bytes(), "The array id should equal the uuid id")
	}
}

func TestUuid_Variant(t *testing.T) {
	for _, v := range namespaces {
		id, _ := Parse(v)
		newId := &uuid{size: 16, node: make([]byte, 6)}
		newId.UnmarshalBinary(id.Bytes())

		assert.NotEqual(t, 0, newId.Variant(), "The variant should be non zero")
	}

	bytes := new(array)
	copy(bytes[:], uuidBytes[:])

	for _, v := range uuidVariants {
		for i := 0; i <= 255; i++ {
			bytes[variantIndex] = byte(i)
			id := createUuid(bytes[:], 4, v)
			b := id.sequenceHiAndVariant >> 4
			tVariantConstraint(v, b, id, t)
			output(id)
			assert.Equal(t, v, id.Variant(), "%x does not resolve to %x", id.Variant(), v)
			output("\n")
		}
	}
}

func TestUuid_Version(t *testing.T) {
	for _, v := range namespaces {

		id, _ := Parse(v)

		newId := &uuid{size: 16, node: make([]byte, 6)}
		newId.UnmarshalBinary(id.Bytes())

		assert.NotEqual(t, 0, newId.Version(), "The version should be non zero")
	}

	id := &uuid{size: length, node: make([]byte, 6)}

	bytes := new(array)
	copy(bytes[:], uuidBytes[:])

	for v := 0; v < 16; v++ {
		for i := 0; i <= 255; i++ {
			bytes[versionIndex] = byte(i)
			id.Unmarshal(bytes[:])
			id.setVersion(uint16(v))
			output(id)
			assert.Equal(t, v, id.Version(), "%x does not resolve to %x", id.Version(), v)
			output("\n")
		}
	}

	assert.True(t, didUuidSetVariantPanic(bytes[:]), "Uuid creation should panic  if invalid variant")

}

func didUuidSetVariantPanic(bytes []byte) bool {
	return func() (didPanic bool) {
		defer func() {
			if recover() != nil {
				didPanic = true
			}
		}()

		createUuid(bytes[:], 4, 0xbb)
		return
	}()
}

// *******************************************************

func createUuid(pData []byte, pVersion uint16, pVariant uint8) *uuid {
	o := &uuid{size: length, node: make([]byte, 6)}
	o.Unmarshal(pData)
	o.setVersion(pVersion)
	o.setVariant(pVariant)
	return o
}
