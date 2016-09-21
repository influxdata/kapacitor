// Overrider provides an API for overriding and reading redacted values for a configuration object.
// The configuration object provided is expected to have two levels of nested structs.
// The "section" level represents the top level fields of the object.
// The "option" level represents the second level of fields in the object.
// Further levels may exist but Overrider will not interact with them.
// A "section" level may be a struct or a slice of structs.
//
// In order for a section to be overridden an `override` struct tag must be present.
// The `override` tag defines a name for the section.
// Struct tags can be used to mark options as redacted by adding a `,redact` to the end of the `override` tag value.
// Overrider also has support for reading option names from custom struct tags like `toml` or `json`
// via the OptionNameFunc field of the Overrider type.
//
// Example:
//    type MySectionConfig struct {
//        Option   string `toml:"option"`
//        Password string `toml:"password" override:",redact"`
//    }
//    type MyConfig struct {
//        SectionName    MySectionConfig      `override:"section-name"`
//        IgnoredSection MyOtherSectionConfig
//    }
//    type MyOtherSectionConfig struct{}
//    // Setup
//    c := MyConfig{
//        MySectionConfig: MySectionConfig{
//             Option: "option value",
//             Password: "secret",
//        },
//        IgnoredSection: MyOtherSectionConfig{},
//    }
//    o := override.New(c)
//    o.OptionNameFunc = override.TomlFieldName
//    // Read redacted section values
//    redacted, err := o.Sections()
//    // Override options for a section
//    newSection, err := o.Override(Override{
//        Section: "section-name",
//        Options: map[string]interface{}{"option": "overridden option value"},
//    })
package override

import (
	"encoding"
	"fmt"
	"log"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/mitchellh/copystructure"
	"github.com/mitchellh/reflectwalk"
	"github.com/pkg/errors"
)

const (
	structTagKey   = "override"
	redactKeyword  = "redact"
	elementKeyword = "element-key="
)

// OptionNameFunc returns the name of a field based on its
// reflect.StructField description.
type OptionNameFunc func(reflect.StructField) string

// TomlFieldName returns the name of a field based on its "toml" struct tag.
// If the tag is absent the Go field name is used.
func TomlFieldName(f reflect.StructField) (name string) {
	return tagFieldName("toml", f)
}

// JSONFieldName returns the name of a field based on its "json" struct tag.
// If the tag is absent the Go field name is used.
func JSONFieldName(f reflect.StructField) (name string) {
	return tagFieldName("json", f)
}

// OverrideFieldName returns the name of a field based on its "override" struct tag.
// If the tag is absent the Go field name is used.
func OverrideFieldName(f reflect.StructField) (name string) {
	return tagFieldName(structTagKey, f)
}

// tagFieldName returns the name of a field based on the value of a given struct tag.
// All content after a "," is ignored.
func tagFieldName(tag string, f reflect.StructField) (name string) {
	parts := strings.Split(f.Tag.Get(tag), ",")
	name = parts[0]
	if name == "" {
		name = f.Name
	}
	return
}

// Overrider provides methods for overriding and reading redacted values for a configuration object.
type Overrider struct {
	// original is the original configuration value provided
	// It is not modified, only copies will be modified.
	original interface{}
	// OptionNameFunc is responsible for determining the names of struct fields.
	OptionNameFunc OptionNameFunc
}

// New Overrider where the config object is expected to have two levels of nested structs.
func New(config interface{}) *Overrider {
	return &Overrider{
		original:       config,
		OptionNameFunc: OverrideFieldName,
	}
}

// Validater is a type that can validate itself
// If a type is a Validater, then Validate() is called
// whenever it is modified.
type Validater interface {
	Validate() error
}

// Override a section with values from the set of overrides.
// The overrides is a map of option name to value.
//
// Values must be of the same type as the named option field, with the exception of numeric types.
// Numeric types will be converted to the absolute type using Go's default conversion mechanisms.
// Strings will also be parsed for numeric values if needed.
// Any mismatched type or failure to convert to a numeric type will result in an error.
//
// The underlying configuration object is not modified, but rather a copy is returned via the Section type.
func (c *Overrider) Override(o Override) (Section, error) {
	// First make a copy into which we can apply the updates.
	copy, err := copystructure.Copy(c.original)
	if err != nil {
		return Section{}, errors.Wrap(err, "failed to copy configuration object")
	}

	return c.applyOverride(copy, o)
}

// applyOverride applies the given override to the specified object.
func (c *Overrider) applyOverride(object interface{}, o Override) (Section, error) {
	if err := o.Validate(); err != nil {
		return Section{}, errors.Wrap(err, "invalid override")
	}
	walker := newOverrideWalker(
		o,
		c.OptionNameFunc,
	)

	// walk the copy and apply the updates
	if err := reflectwalk.Walk(object, walker); err != nil {
		return Section{}, errors.Wrapf(err, "failed to apply changes to configuration object for section %s", o.Section)
	}
	unused := walker.unused()
	if len(unused) > 0 {
		return Section{}, fmt.Errorf("unknown options %v in section %s", unused, o.Section)
	}
	// Return the modified copy
	section := walker.sectionObject()
	if section.value == nil && !o.Delete {
		return Section{}, fmt.Errorf("unknown section %s", o.Section)
	}
	// Validate new value
	if v, ok := section.value.(Validater); ok {
		if err := v.Validate(); err != nil {
			return Section{}, errors.Wrap(err, "failed validation")
		}
	}
	return section, nil
}

type Override struct {
	Section string
	Element string
	Options map[string]interface{}
	Delete  bool
	Create  bool
}

func (o Override) Validate() error {
	if o.Section == "" {
		return errors.New("section cannot be empty")
	}
	if o.Delete && o.Element == "" {
		return errors.New("element cannot be empty if deleting an element")
	}
	if o.Create && o.Element != "" {
		return errors.New("element must be empty if creating an element, set the element key value via the options")
	}
	if o.Delete && len(o.Options) > 0 {
		return errors.New("cannot delete an element and provide options in the same override")
	}
	if o.Delete && o.Create {
		return errors.New("cannot create and delete an element in the same override")
	}
	return nil
}

// OverrideAll applies all given overrides and returns a map of all configuration sections, even if they were not overridden.
// The overrides are all applied to the same object and the original configuration object remains unmodified.
func (c *Overrider) OverrideAll(os []Override) (map[string]SectionList, error) {
	// First make a copy into which we can apply the updates.
	copy, err := copystructure.Copy(c.original)
	if err != nil {
		return nil, errors.Wrap(err, "failed to copy configuration object")
	}

	// Apply all overrides to the same copy
	for _, o := range os {
		// We do not need to keep a reference to the section since we are going to walk the entire copy next
		_, err := c.applyOverride(copy, o)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to override configuration section/element %s/%s", o.Section, o.Element)
		}
	}

	// Walk the copy to return all sections
	walker := newSectionWalker(c.OptionNameFunc)
	if err := reflectwalk.Walk(copy, walker); err != nil {
		return nil, errors.Wrap(err, "failed to read sections from configuration object")
	}

	return walker.sectionsMap(), nil
}

// Sections returns the original unmodified configuration sections.
func (c *Overrider) Sections() (map[string]SectionList, error) {
	// walk the original and read all sections
	walker := newSectionWalker(c.OptionNameFunc)
	if err := reflectwalk.Walk(c.original, walker); err != nil {
		return nil, errors.Wrap(err, "failed to read sections from configuration object")
	}

	return walker.sectionsMap(), nil
}

// ElementKeys returns a map of section name to element key name for each section.
func (c *Overrider) ElementKeys() (map[string]string, error) {
	// walk the original and read all sections
	walker := newSectionWalker(c.OptionNameFunc)
	if err := reflectwalk.Walk(c.original, walker); err != nil {
		return nil, errors.Wrap(err, "failed to read sections from configuration object")
	}

	return walker.elementKeysMap(), nil
}

// overrideWalker applies the changes onto the walked value.
type overrideWalker struct {
	depthWalker

	o Override

	optionNameFunc OptionNameFunc

	used               map[string]bool
	sectionValue       reflect.Value
	currentSectionName string
	currentElementName string
	currentSlice       reflect.Value
	elementKey         string
}

func newOverrideWalker(o Override, optionNameFunc OptionNameFunc) *overrideWalker {
	return &overrideWalker{
		o:              o,
		used:           make(map[string]bool, len(o.Options)),
		optionNameFunc: optionNameFunc,
	}
}

func (w *overrideWalker) unused() []string {
	unused := make([]string, 0, len(w.o.Options))
	for name := range w.o.Options {
		if !w.used[name] {
			unused = append(unused, name)
		}
	}
	return unused
}

func (w *overrideWalker) sectionObject() Section {
	if w.sectionValue.IsValid() {
		return Section{
			value:          w.sectionValue.Interface(),
			optionNameFunc: w.optionNameFunc,
			element:        w.o.Element,
		}
	}
	return Section{}
}

func (w *overrideWalker) Struct(reflect.Value) error {
	return nil
}

func (w *overrideWalker) StructField(f reflect.StructField, v reflect.Value) error {
	switch w.depth {
	// Section level
	case 0:
		name, ok := getSectionName(f)
		if ok {
			// Only override the section if a struct tag was present
			w.currentSectionName = name
			if w.o.Section == w.currentSectionName {
				w.sectionValue = v
				w.elementKey = getElementKey(f)
			}
		} else {
			w.currentSectionName = ""
		}
	// Option level
	case 1:
		// Skip this field if its not for the section/element we care about
		if w.currentSectionName != w.o.Section || w.currentElementName != w.o.Element {
			break
		}

		name := w.optionNameFunc(f)
		setValue, ok := w.o.Options[name]
		if ok {
			if !w.o.Create && name == w.elementKey {
				return fmt.Errorf("cannot override element key %s", name)
			}
			log.Println("D! copy option", name)
			if err := weakCopyValue(reflect.ValueOf(setValue), v); err != nil {
				return errors.Wrapf(err, "cannot set option %s", name)
			}
			w.used[name] = true
		}
	}
	return nil
}

// Defaulter set defaults on the receiving object.
// If a type is a Defaulter and a new value needs to be created of that type,
// then Default() is called on a new instance of that type.
type Defaulter interface {
	SetDefaults()
}

var defaulterType = reflect.TypeOf((*Defaulter)(nil)).Elem()

func (w *overrideWalker) Slice(v reflect.Value) error {
	if w.o.Section != w.currentSectionName || w.depth != 1 {
		return nil
	}
	w.currentSlice = v
	switch {
	case w.o.Delete:
		// Explictly set the section value to the zero value
		w.sectionValue = reflect.Value{}
	case w.o.Create:
		// Create a new element in the slice
		var n reflect.Value
		et := v.Type().Elem()
		if et.Kind() == reflect.Ptr {
			n = reflect.New(et.Elem())
		} else {
			n = reflect.New(et)
		}
		// If the type is a defaulter, call Default
		if n.Type().Implements(defaulterType) {
			n.Interface().(Defaulter).SetDefaults()
		}
		// Indirect the value if we didn't want a pointer
		if et.Kind() != reflect.Ptr {
			n = reflect.Indirect(n)
		}
		v.Set(reflect.Append(v, n))
		// Set element key
		if w.elementKey != "" {
			// Get the value that is now part of the slice.
			n = v.Index(v.Len() - 1)

			elementField := findFieldByElementKey(n, w.elementKey, w.optionNameFunc)
			if elementField.IsValid() {
				if elementField.Kind() != reflect.String {
					return fmt.Errorf("element key field must be of type string, got %s", elementField.Type())
				}
				if setValue, ok := w.o.Options[w.elementKey]; ok {
					if str, ok := setValue.(string); ok {
						w.o.Element = str
						w.used[w.elementKey] = true
					} else {
						return fmt.Errorf("type of element key must be a string, got %T ", setValue)
					}
					if err := weakCopyValue(reflect.ValueOf(setValue), elementField); err != nil {
						return errors.Wrapf(err, "cannot set element key %q on new element", w.elementKey)
					}
				} else {
					return fmt.Errorf("element key %q not present in options", w.elementKey)
				}
			} else {
				return fmt.Errorf("could not find field with the name of the element key %q", w.elementKey)
			}
		} else {
			return fmt.Errorf("cannot create new element, no element key found. An element key must be specified via the `%s:\",%s<field name>\"` struct tag", structTagKey, elementKeyword)
		}
	default:
		// We are modifying an existing slice element.
		// Nothing to do here.
	}
	return nil
}

func (w *overrideWalker) SliceElem(idx int, v reflect.Value) error {
	if w.depth == 1 && w.currentSectionName == w.o.Section && w.o.Element != "" {
		w.currentElementName = ""
		if w.elementKey != "" {
			// Get current element name via field on current value
			elementField := findFieldByElementKey(v, w.elementKey, w.optionNameFunc)
			if elementField.IsValid() {
				if elementField.Kind() != reflect.String {
					return fmt.Errorf("element key field must be of type string, got %s", elementField.Type())
				}
				w.currentElementName = elementField.String()
				if w.o.Element == w.currentElementName {
					if w.o.Delete {
						// Delete the element from the slice by re-slicing the element out
						w.currentSlice.Set(
							reflect.AppendSlice(
								w.currentSlice.Slice(0, idx),
								w.currentSlice.Slice(idx+1, w.currentSlice.Len()),
							),
						)
					} else {
						w.sectionValue = v
					}
				}
			} else {
				return fmt.Errorf("could not find field with name %q on value of type %s", w.elementKey, v.Type())
			}
		} else {
			return fmt.Errorf("an element key must be specified via the `%s:\",%s<field name>\"` struct tag", structTagKey, elementKeyword)
		}
	}
	return nil
}

var textUnmarshalerType = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()

// weakCopyValue copies the value of dst into src, where numeric and interface types are copied weakly.
func weakCopyValue(src, dst reflect.Value) (err error) {
	defer func() {
		// This shouldn't be necessary but it better to catch a panic here,
		// where we can provide context instead of crashing the server or letting it bouble up.
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				err = e
			} else {
				err = fmt.Errorf("%v", r)
			}
		}
	}()
	if !dst.CanSet() {
		return errors.New("not settable")
	}
	if src.Kind() == reflect.Interface {
		src = src.Elem()
	}
	srcK := src.Kind()
	dstK := dst.Kind()

	// Get addressable value since it may implement
	// the TextUnmarshaler interface
	var addrDst reflect.Value
	if d := reflect.Indirect(dst); d.CanAddr() {
		addrDst = d.Addr()
	}

	if dstK == reflect.Int64 {
		log.Println("D! weakCopyValue", dst, dst.Type())
	}
	if srcK == dstK {
		if dst.Type() == src.Type() {
			// Perform normal copy
			dst.Set(src)
		} else {
			// Perform recursive copy into elements
			switch dstK {
			case reflect.Map:
				if dst.IsNil() {
					dst.Set(reflect.MakeMap(dst.Type()))
				}
				for _, key := range src.MapKeys() {
					value := reflect.Indirect(reflect.New(dst.Type().Elem()))
					if err := weakCopyValue(src.MapIndex(key), value); err != nil {
						return errors.Wrap(err, "failed to copy map value")
					}
					dst.SetMapIndex(key, value)
				}
			case reflect.Slice, reflect.Array:
				if dstK == reflect.Slice && dst.IsNil() {
					dst.Set(reflect.MakeSlice(dst.Type(), src.Len(), src.Len()))
				}
				for i := 0; i < src.Len(); i++ {
					value := reflect.Indirect(reflect.New(dst.Type().Elem()))
					if err := weakCopyValue(src.Index(i), value); err != nil {
						return errors.Wrap(err, "failed to copy slice value")
					}
					dst.Index(i).Set(value)
				}
			default:
				return fmt.Errorf("cannot copy mismatched types got %s exp %s", src.Type().String(), dst.Type().String())
			}
		}
	} else if addrDst.Type().Implements(textUnmarshalerType) {
		um := addrDst.Interface().(encoding.TextUnmarshaler)
		var text []byte
		if src.Type().Implements(stringerType) || srcK == reflect.String {
			text = []byte(src.String())
		} else {
			return fmt.Errorf("cannot unmarshal %s into %s", srcK, dstK)
		}
		if err := um.UnmarshalText(text); err != nil {
			errors.Wrap(err, "failed to unmarshal text")
		}
	} else if isNumericKind(dstK) {
		// Perform weak numeric copy
		if isNumericKind(srcK) {
			dst.Set(src.Convert(dst.Type()))
			return nil
		} else {
			var str string
			if src.Type().Implements(stringerType) || srcK == reflect.String {
				str = src.String()
			} else {
				return fmt.Errorf("cannot convert %s into %s", srcK, dstK)
			}
			switch {
			case isIntKind(dstK):
				if i, err := strconv.ParseInt(str, 10, 64); err == nil {
					dst.SetInt(i)
					return nil
				}
			case isUintKind(dstK):
				if i, err := strconv.ParseUint(str, 10, 64); err == nil {
					dst.SetUint(i)
					return nil
				}
			case isFloatKind(dstK):
				if f, err := strconv.ParseFloat(str, 64); err == nil {
					dst.SetFloat(f)
					return nil
				}
			}
			return fmt.Errorf("cannot convert string %q into %s", str, dstK)
		}
	} else {
		return fmt.Errorf("wrong kind %s, expected value of kind %s: %t", srcK, dstK, srcK == dstK)
	}
	return nil
}

// Stringer is a type that can provide a string value of itself.
// If a value is a Stringer and needs to be copied into a numeric value,
// then String() is called and parsed as a numeric value if possible.
type Stringer interface {
	String() string
}

var stringerType = reflect.TypeOf((*Stringer)(nil)).Elem()

func isNumericKind(k reflect.Kind) bool {
	// Ignoring complex kinds since we cannot convert them
	return k >= reflect.Int && k <= reflect.Float64
}
func isIntKind(k reflect.Kind) bool {
	return k >= reflect.Int && k <= reflect.Int64
}
func isUintKind(k reflect.Kind) bool {
	return k >= reflect.Uint && k <= reflect.Uint64
}
func isFloatKind(k reflect.Kind) bool {
	return k == reflect.Float32 || k == reflect.Float64
}

// Section provides access to the underlying value or a map of redacted values.
type Section struct {
	value          interface{}
	element        string
	optionNameFunc OptionNameFunc
}

// elementValue returns unique value that identifies this element
func (s Section) elementValue() string {
	return s.element
}

// Value returns the underlying value.
func (s Section) Value() interface{} {
	return s.value
}

// Redacted returns the options for the section in a map.
// Any fields with the `override:",redact"` tag set will be replaced
// with a boolean value indicating whether a non-zero value was set.
func (s Section) Redacted() (map[string]interface{}, error) {
	walker := newRedactWalker(s.optionNameFunc)
	// walk the section and collect redacted options
	if err := reflectwalk.Walk(s.value, walker); err != nil {
		return nil, errors.Wrap(err, "failed to redact section")
	}
	return walker.optionsMap(), nil
}

// getElementKey returns the name of the field taht is used to uniquely identify elements of a list.
func getElementKey(f reflect.StructField) string {
	parts := strings.Split(f.Tag.Get(structTagKey), ",")
	if len(parts) > 1 {
		for _, p := range parts[1:] {
			if strings.HasPrefix(p, elementKeyword) {
				return strings.TrimPrefix(p, elementKeyword)
			}
		}
	}
	return ""
}

func findFieldByElementKey(v reflect.Value, elementKey string, optionNameFunc OptionNameFunc) (field reflect.Value) {
	v = reflect.Indirect(v)
	if v.Kind() != reflect.Struct {
		log.Println("value is not a struct", v)
		return
	}
	field = v.FieldByName(elementKey)
	if field.IsValid() {
		return
	}

	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		field = v.Field(i)
		// Skip any unexported fields
		if !field.CanSet() {
			continue
		}
		name := optionNameFunc(t.Field(i))
		if name == elementKey {
			return
		}
	}
	return
}

// redactWalker reads the the sections from the walked values and redacts and sensitive fields.
type redactWalker struct {
	depthWalker
	options        map[string]interface{}
	optionNameFunc OptionNameFunc
}

func newRedactWalker(optionNameFunc OptionNameFunc) *redactWalker {
	return &redactWalker{
		options:        make(map[string]interface{}),
		optionNameFunc: optionNameFunc,
	}
}

func (w *redactWalker) optionsMap() map[string]interface{} {
	return w.options
}

func (w *redactWalker) Struct(reflect.Value) error {
	return nil
}

func (w *redactWalker) StructField(f reflect.StructField, v reflect.Value) error {
	switch w.depth {
	// Top level
	case 0:
		name := w.optionNameFunc(f)
		w.options[name] = getRedactedValue(f, v)
	// Ignore all other levels
	default:
	}
	return nil
}

func getRedactedValue(f reflect.StructField, v reflect.Value) interface{} {
	if isRedacted(f) {
		return !isZero(v)
	} else {
		return v.Interface()
	}
}

func isRedacted(f reflect.StructField) bool {
	parts := strings.Split(f.Tag.Get(structTagKey), ",")
	if len(parts) > 1 {
		for _, p := range parts[1:] {
			if p == redactKeyword {
				return true
			}
		}
	}
	return false
}

// isZero returns whether if v is equal to the zero value of its type.
func isZero(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Func, reflect.Map, reflect.Slice:
		return v.IsNil()
	case reflect.Array:
		// Check arrays linearly since its element type may not be comparable.
		z := true
		for i := 0; i < v.Len() && z; i++ {
			z = z && isZero(v.Index(i))
		}
		return z
	case reflect.Struct:
		// Check structs recusively since not all of its field may be comparable
		z := true
		for i := 0; i < v.NumField() && z; i++ {
			if f := v.Field(i); f.CanSet() {
				z = z && isZero(f)
			}
		}
		return z
	default:
		// Compare other types directly:
		z := reflect.Zero(v.Type())
		return v.Interface() == z.Interface()
	}
}

type SectionList []Section

func (s SectionList) Len() int           { return len(s) }
func (s SectionList) Less(i, j int) bool { return s[i].elementValue() < s[j].elementValue() }
func (s SectionList) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// sectionWalker reads the sections from the walked values and redacts any sensitive fields.
type sectionWalker struct {
	depthWalker
	sections           map[string]SectionList
	optionNameFunc     OptionNameFunc
	currentSectionName string
	elementKeys        map[string]string
}

func newSectionWalker(optionNameFunc OptionNameFunc) *sectionWalker {
	return &sectionWalker{
		sections:       make(map[string]SectionList),
		elementKeys:    make(map[string]string),
		optionNameFunc: optionNameFunc,
	}
}

func (w *sectionWalker) sectionsMap() map[string]SectionList {
	for _, sectionList := range w.sections {
		sort.Sort(sectionList)
	}
	return w.sections
}

func (w *sectionWalker) elementKeysMap() map[string]string {
	return w.elementKeys
}

func (w *sectionWalker) Struct(reflect.Value) error {
	return nil
}

func (w *sectionWalker) StructField(f reflect.StructField, v reflect.Value) error {
	switch w.depth {
	// Section level
	case 0:
		name, ok := getSectionName(f)
		if ok {
			w.currentSectionName = name
			elementKey := getElementKey(f)
			w.elementKeys[name] = elementKey
			if k := reflect.Indirect(v).Kind(); k == reflect.Struct {
				w.sections[name] = SectionList{{
					value:          v.Interface(),
					optionNameFunc: w.optionNameFunc,
				}}
			}
		} else {
			w.currentSectionName = ""
		}
	// Skip all other levels
	default:
	}
	return nil
}

func (w *sectionWalker) Slice(reflect.Value) error {
	return nil
}

func (w *sectionWalker) SliceElem(idx int, v reflect.Value) error {
	// Skip sections that we are not interested in
	if w.currentSectionName == "" {
		return nil
	}
	switch w.depth {
	//Option level
	case 1:
		// Get element value from object
		var element string
		elementKey, ok := w.elementKeys[w.currentSectionName]
		if !ok {
			return fmt.Errorf("no element key found for section %q, %v", w.currentSectionName, v)
		}
		elementField := findFieldByElementKey(v, elementKey, w.optionNameFunc)
		if elementField.IsValid() {
			if elementField.Kind() != reflect.String {
				return fmt.Errorf("element key field must be of type string, got %s", elementField.Type())
			}
			element = elementField.String()
		} else {
			return fmt.Errorf("could not find field with the name of the element key %q on element object", elementKey)
		}
		w.sections[w.currentSectionName] = append(w.sections[w.currentSectionName], Section{
			value:          v.Interface(),
			optionNameFunc: w.optionNameFunc,
			element:        element,
		})
		// Skip all other levels
	default:
	}
	return nil
}

// getSectionName returns the name of the section based off its `override` struct tag.
// If no tag is present the Go field name is returned and the second return value is false.
func getSectionName(f reflect.StructField) (string, bool) {
	parts := strings.Split(f.Tag.Get(structTagKey), ",")
	if parts[0] != "" {
		return parts[0], true
	}
	return f.Name, false
}

// depthWalker keeps track of the current depth count into nested structs.
type depthWalker struct {
	depth int
}

func (w *depthWalker) Enter(l reflectwalk.Location) error {
	if l == reflectwalk.StructField {
		w.depth++
	}
	return nil
}

func (w *depthWalker) Exit(l reflectwalk.Location) error {
	if l == reflectwalk.StructField {
		w.depth--
	}
	return nil
}
