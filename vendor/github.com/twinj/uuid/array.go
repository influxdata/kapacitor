package uuid

/****************
 * Date: 1/02/14
 * Time: 10:08 AM
 ***************/

const (
	variantIndex = 8
	versionIndex = 6
)

// A clean UUID type for simpler UUID versions
type array [length]byte

func (array) Size() int {
	return length
}

func (o array) Version() int {
	return int(o[versionIndex]) >> 4
}

func (o *array) setVersion(pVersion byte) {
	o[versionIndex] &= 0x0f
	o[versionIndex] |= pVersion << 4
}

func (o *array) Variant() byte {
	return variant(o[variantIndex])
}

func (o *array) setVariant(pVariant byte) {
	setVariant(&o[variantIndex], pVariant)
}

func (o *array) Unmarshal(pData []byte) {
	copy(o[:], pData)
}

func (o *array) Bytes() []byte {
	return o[:]
}

func (o array) String() string {
	return formatter(&o, generator.format)
}

func (o array) Format(pFormat string) string {
	return formatter(&o, pFormat)
}

// Set the three most significant bits (bits 0, 1 and 2) of the
// sequenceHiAndVariant equivalent in the array to ReservedRFC4122.
func (o *array) setRFC4122Variant() {
	o[variantIndex] &= 0x3F
	o[variantIndex] |= ReservedRFC4122
}

// Marshals the UUID bytes into a slice
func (o *array) MarshalBinary() ([]byte, error) {
	return o.Bytes(), nil
}

// Un-marshals the data bytes into the UUID.
func (o *array) UnmarshalBinary(pData []byte) error {
	return UnmarshalBinary(o, pData)
}
