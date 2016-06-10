package uuid

import (
	"encoding/binary"
)

/****************
 * Date: 31/01/14
 * Time: 3:34 PM
 ***************/

var _ UUID = &uuid{}

// uuid is used for RFC4122 Version 1 UUIDs
type uuid struct {
	timeLow              uint32
	timeMid              uint16
	timeHiAndVersion     uint16
	sequenceHiAndVariant uint8
	sequenceLow          uint8
	size                 uint8
	node                 []byte
}

func (o uuid) Size() int {
	return int(o.size)
}

func (o uuid) Version() int {
	return int(o.timeHiAndVersion >> 12)
}

func (o uuid) Variant() uint8 {
	return variant(o.sequenceHiAndVariant)
}

func (o *uuid) Unmarshal(pData []byte) {

	o.timeLow = binary.BigEndian.Uint32(pData[:4])
	o.timeMid = binary.BigEndian.Uint16(pData[4:6])
	o.timeHiAndVersion = binary.BigEndian.Uint16(pData[6:8])

	o.sequenceHiAndVariant = pData[8]
	o.sequenceLow = pData[9]

	copy(o.node[:], pData[10:o.size])
}

func (o uuid) Bytes() (data []byte) {

	data = make([]byte, o.size)

	binary.BigEndian.PutUint32(data[:4], o.timeLow)
	binary.BigEndian.PutUint16(data[4:6], o.timeMid)
	binary.BigEndian.PutUint16(data[6:8], o.timeHiAndVersion)

	data[8] = o.sequenceHiAndVariant
	data[9] = o.sequenceLow

	copy(data[10:o.size], o.node[:])

	return
}

// Marshals the UUID bytes into a slice
func (o uuid) MarshalBinary() ([]byte, error) {
	return o.Bytes(), nil
}

// Un-marshals the data bytes into the UUID struct.
// Implements the BinaryUn-marshaller interface
func (o *uuid) UnmarshalBinary(pData []byte) error {
	return UnmarshalBinary(o, pData)
}

func (o uuid) String() string {
	return formatter(&o, generator.Fmt)
}

// ****************************************************

// Sets the four most significant bits (bits 12 through 15) of the
// timeHiAndVersion field to the 4-bit version number.
func (o *uuid) setVersion(pVersion uint16) {
	o.timeHiAndVersion &= 0x0fff
	o.timeHiAndVersion |= pVersion << 12
}

func (o *uuid) setVariant(pVariant uint8) {
	setVariant(&o.sequenceHiAndVariant, pVariant)
}
