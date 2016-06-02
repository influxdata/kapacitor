package uuid

import "encoding/binary"

/****************
 * Date: 31/01/14
 * Time: 3:34 PM
 ***************/

// uuid is used for RFC4122 Version 1 UUIDs
type uuid struct {
	timeLow              uint32
	timeMid              uint16
	timeHiAndVersion     uint16
	sequenceHiAndVariant byte
	sequenceLow          byte
	node                 []byte
	size                 int
}

func (o uuid) Size() int {
	return o.size
}

func (o uuid) Version() int {
	return int(o.timeHiAndVersion >> 12)
}

func (o uuid) Variant() byte {
	return variant(o.sequenceHiAndVariant)
}

// Sets the four most significant bits (bits 12 through 15) of the
// timeHiAndVersion field to the 4-bit version number.
func (o *uuid) setVersion(pVersion int) {
	o.timeHiAndVersion &= 0x0FFF
	o.timeHiAndVersion |= (uint16(pVersion) << 12)
}

func (o *uuid) setVariant(pVariant byte) {
	setVariant(&o.sequenceHiAndVariant, pVariant)
}

func (o *uuid) Unmarshal(pData []byte) {

	o.timeLow = binary.BigEndian.Uint32(pData[:4])
	o.timeMid = binary.BigEndian.Uint16(pData[4:6])
	o.timeHiAndVersion = binary.BigEndian.Uint16(pData[6:8])
	o.sequenceHiAndVariant = pData[8]
	o.sequenceLow = pData[9]
	copy(o.node, pData[10:o.Size()])
}

func (o *uuid) Bytes() (data []byte) {

	data = make([]byte, o.size)
	binary.BigEndian.PutUint32(data[:4], o.timeLow)
	binary.BigEndian.PutUint16(data[4:6], o.timeMid)
	binary.BigEndian.PutUint16(data[6:8], o.timeHiAndVersion)
	data[8] = o.sequenceHiAndVariant
	data[9] = o.sequenceLow

	copy(data[10:], o.node)

	return
}

// Marshals the UUID bytes into a slice
func (o *uuid) MarshalBinary() ([]byte, error) {
	return o.Bytes(), nil
}

// Un-marshals the data bytes into the UUID struct.
// Implements the BinaryUn-marshaller interface
func (o *uuid) UnmarshalBinary(pData []byte) error {
	return UnmarshalBinary(o, pData)
}

func (o uuid) String() string {
	return formatter(&o, generator.format)
}

func (o uuid) Format(pFormat string) string {
	return formatter(&o, pFormat)
}

// Set the three most significant bits (bits 0, 1 and 2) of the
// sequenceHiAndVariant to variant mask 0x80.
func (o *uuid) setRFC4122Variant() {
	o.sequenceHiAndVariant &= variantSet
	o.sequenceHiAndVariant |= ReservedRFC4122
}
