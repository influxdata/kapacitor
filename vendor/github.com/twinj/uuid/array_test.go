package uuid

/****************
 * Date: 15/02/14
 * Time: 12:49 PM
 ***************/

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestArray_Bytes(t *testing.T) {
	id := array(uuidBytes)
	assert.Equal(t, id[:], uuidId.Bytes(), "Bytes should be the same")
}

func TestArray_Unmarshal(t *testing.T) {
	id := array(uuidBytes)
	id2 := &array{}
	id2.Unmarshal(uuidBytes[:])

	assert.Equal(t, id2.String(), id.String(), "String should be the same")
}

func TestArray_MarshalBinary(t *testing.T) {
	id := array(uuidBytes)
	bytes, err := id.MarshalBinary()
	assert.Nil(t, err, "There should be no error")
	assert.Equal(t, uuidBytes[:], bytes, "Byte should be the same")
}

func TestArray_Size(t *testing.T) {
	id := &array{}
	assert.Equal(t, 16, id.Size(), "The size of the array should be sixteen")
}

func TestArray_String(t *testing.T) {
	id := array(uuidBytes)
	assert.Equal(t, idString, id.String(), "The Format given should match the output")
}

func TestArray_UnmarshalBinary(t *testing.T) {

	u := new(array)

	err := u.UnmarshalBinary([]byte{1, 2, 3, 4, 5})

	assert.Equal(t, "uuid.UnmarshalBinary: invalid length", err.Error(), "Expect length error")

	err = u.UnmarshalBinary(uuidBytes[:])

	assert.Nil(t, err, "There should be no error but got %s", err)

	for k, v := range namespaces {
		id, _ := Parse(v)
		uuidId := &array{}
		uuidId.UnmarshalBinary(id.Bytes())

		assert.Equal(t, id.Bytes(), uuidId.Bytes(), "The array id should equal the uuid id")
		assert.Equal(t, k.Bytes(), uuidId.Bytes(), "The array id should equal the uuid id")
	}
}

func TestArray_Variant(t *testing.T) {
	for _, v := range namespaces {
		id, _ := Parse(v)
		uuidId := &array{}
		uuidId.UnmarshalBinary(id.Bytes())

		assert.NotEqual(t, 0, uuidId.Variant(), "The variant should be non zero")
	}

	bytes := new(array)
	copy(bytes[:], uuidBytes[:])

	for _, v := range uuidVariants {
		for i := 0; i <= 255; i++ {
			bytes[variantIndex] = byte(i)
			id := createArray(bytes[:], 4, v)
			b := id[variantIndex] >> 4
			tVariantConstraint(v, b, id, t)
			output(id)
			assert.Equal(t, v, id.Variant(), "%x does not resolve to %x", id.Variant(), v)
			output("\n")
		}
	}

	assert.True(t, didArraySetVariantPanic(bytes[:]), "Array creation should panic  if invalid variant")
}

func didArraySetVariantPanic(bytes []byte) bool {
	return func() (didPanic bool) {
		defer func() {
			if recover() != nil {
				didPanic = true
			}
		}()

		createArray(bytes[:], 4, 0xbb)
		return
	}()
}

func TestArray_Version(t *testing.T) {
	for _, v := range namespaces {
		id, _ := Parse(v)
		uuidId := &array{}
		uuidId.UnmarshalBinary(id.Bytes())

		assert.NotEqual(t, 0, uuidId.Version(), "The version should be non zero")
	}

	id := &array{}

	bytes := new(array)
	copy(bytes[:], uuidBytes[:])

	for v := 0; v < 16; v++ {
		for i := 0; i <= 255; i++ {
			bytes[versionIndex] = byte(i)
			id.Unmarshal(bytes[:])
			id.setVersion(uint8(v))
			output(id)
			assert.Equal(t, v, id.Version(), "%x does not resolve to %x", id.Version(), v)
			output("\n")
		}
	}
}

// *******************************************************

func createArray(pData []byte, pVersion uint8, pVariant uint8) *array {
	o := new(array)
	o.Unmarshal(pData)
	o.setVersion(pVersion)
	o.setVariant(pVariant)
	return o
}
