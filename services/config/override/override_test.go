package override_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/influxdata/kapacitor/services/config/override"
	"github.com/mitchellh/copystructure"
)

type SectionA struct {
	Option1 string `toml:"toml-option1" json:"json-option1"`
	Option2 string `toml:"toml-option2" json:"json-option2"`
}
type SectionB struct {
	Option3 string `toml:"toml-option3" json:"json-option3"`
}
type SectionC struct {
	Option4  int64  `toml:"toml-option4" json:"json-option4"`
	Password string `toml:"toml-password" json:"json-password" override:",redact"`
}

type SectionD struct {
	ID      string                    `toml:"toml-id" json:"json-id"`
	Option5 string                    `toml:"toml-option5" json:"json-option5"`
	Option6 map[string]map[string]int `toml:"toml-option6" json:"json-option6"`
	Option7 [][]int                   `toml:"toml-option7" json:"json-option7"`
}

func (d *SectionD) SetDefaults() {
	d.Option5 = "o5"
}

func (d SectionD) Validate() error {
	if d.ID == "" {
		return fmt.Errorf("ID cannot be empty")
	}
	return nil
}

type SectionIgnored struct {
	String string
}

type SectionNums struct {
	Int   int
	Int8  int8
	Int16 int16
	Int32 int32
	Int64 int64

	Uint   uint
	Uint8  uint8
	Uint16 uint16
	Uint32 uint32
	Uint64 uint64

	Float32 float32
	Float64 float64
}

type TestConfig struct {
	SectionA       SectionA    `override:"section-a"`
	SectionB       SectionB    `override:"section-b"`
	SectionC       *SectionC   `override:"section-c"`
	SectionNums    SectionNums `override:"section-nums"`
	SectionDs      []SectionD  `override:"section-d,element-key=ID"`
	SectionIgnored SectionIgnored
	IgnoredInt     int
	IgnoredString  string
}

func ExampleOverrider() {
	config := &TestConfig{
		SectionA: SectionA{
			Option1: "o1",
		},
		SectionB: SectionB{
			Option3: "o2",
		},
		SectionC: &SectionC{
			Option4: -1,
		},
		SectionDs: []SectionD{
			{
				ID:      "x",
				Option5: "x-5",
			},
			{
				ID:      "y",
				Option5: "y-5",
			},
			{
				ID:      "z",
				Option5: "z-5",
			},
		},
	}

	// Create new ConfigOverrider
	cu := override.New(config)
	// Use toml tags to map field names
	cu.OptionNameFunc = override.TomlFieldName

	// Override options in section-a
	if newSectionA, err := cu.Override(override.Override{
		Section: "section-a",
		Options: map[string]interface{}{
			"toml-option1": "new option1 value",
			"toml-option2": "initial option2 value",
		}}); err != nil {
		fmt.Println("ERROR:", err)
	} else {
		a := newSectionA.Value().(SectionA)
		fmt.Println("New SectionA.Option1:", a.Option1)
		fmt.Println("New SectionA.Option2:", a.Option2)
	}

	// Override options in section-b
	if newSectionB, err := cu.Override(override.Override{
		Section: "section-b",
		Options: map[string]interface{}{
			"toml-option3": "initial option3 value",
		}}); err != nil {
		fmt.Println("ERROR:", err)
	} else {
		b := newSectionB.Value().(SectionB)
		fmt.Println("New SectionB.Option3:", b.Option3)

	}
	// Override options in section-c
	if newSectionC, err := cu.Override(override.Override{
		Section: "section-c",
		Options: map[string]interface{}{
			"toml-option4": 586,
		}}); err != nil {
		fmt.Println("ERROR:", err)
	} else {
		c := newSectionC.Value().(*SectionC)
		fmt.Println("New SectionC.Option4:", c.Option4)

	}
	// Override options in section-d
	if newSectionD, err := cu.Override(override.Override{
		Section: "section-d",
		Element: "x",
		Options: map[string]interface{}{
			"toml-option5": "x-new-5",
		}}); err != nil {
		fmt.Println("ERROR:", err)
	} else {
		d := newSectionD.Value().(SectionD)
		fmt.Println("New SectionD[0].Option5:", d.Option5)
	}

	if newSectionD, err := cu.Override(override.Override{
		Section: "section-d",
		Element: "y",
		Options: map[string]interface{}{
			"toml-option5": "y-new-5",
		}}); err != nil {
		fmt.Println("ERROR:", err)
	} else {
		d := newSectionD.Value().(SectionD)
		fmt.Println("New SectionD[1].Option5:", d.Option5)
	}

	// Create new element in section-d
	if newSectionD, err := cu.Override(override.Override{
		Section: "section-d",
		Create:  true,
		Options: map[string]interface{}{
			"ID":           "w",
			"toml-option5": "w-new-5",
		}}); err != nil {
		fmt.Println("ERROR:", err)
	} else {
		d := newSectionD.Value().(SectionD)
		fmt.Println("New SectionD[3].Option5:", d.Option5)
	}

	//Output:
	// New SectionA.Option1: new option1 value
	// New SectionA.Option2: initial option2 value
	// New SectionB.Option3: initial option3 value
	// New SectionC.Option4: 586
	// New SectionD[0].Option5: x-new-5
	// New SectionD[1].Option5: y-new-5
	// New SectionD[3].Option5: w-new-5
}

func TestOverrider_Override(t *testing.T) {
	testConfig := &TestConfig{
		SectionA: SectionA{
			Option1: "o1",
		},
		SectionC: &SectionC{
			Option4: -1,
		},
		SectionDs: []SectionD{
			{
				ID:      "x",
				Option5: "x-5",
			},
			{
				ID:      "y",
				Option5: "y-5",
			},
			{
				ID:      "z",
				Option5: "z-5",
			},
		},
	}
	copy, err := copystructure.Copy(testConfig)
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		o              override.Override
		exp            interface{}
		redacted       map[string]interface{}
		optionNameFunc override.OptionNameFunc
	}{
		{
			o: override.Override{
				Section: "section-a",
				Options: map[string]interface{}{
					"Option1": "new-o1",
				},
			},
			exp: SectionA{
				Option1: "new-o1",
			},
			redacted: map[string]interface{}{
				"Option1": "new-o1",
				"Option2": "",
			},
		},
		{
			o: override.Override{
				Section: "section-a",
				Options: map[string]interface{}{
					"toml-option1": "new-o1",
				},
			},
			optionNameFunc: override.TomlFieldName,
			exp: SectionA{
				Option1: "new-o1",
			},
			redacted: map[string]interface{}{
				"toml-option1": "new-o1",
				"toml-option2": "",
			},
		},
		{
			o: override.Override{
				Section: "section-a",
				Options: map[string]interface{}{
					"json-option1": "new-o1",
				},
			},
			optionNameFunc: override.JSONFieldName,
			exp: SectionA{
				Option1: "new-o1",
			},
			redacted: map[string]interface{}{
				"json-option1": "new-o1",
				"json-option2": "",
			},
		},
		{
			o: override.Override{
				Section: "section-c",
				Options: map[string]interface{}{
					"toml-option4": 42,
				},
			},
			optionNameFunc: override.TomlFieldName,
			exp: &SectionC{
				Option4: 42,
			},
			redacted: map[string]interface{}{
				"toml-option4":  int64(42),
				"toml-password": false,
			},
		},
		{
			o: override.Override{
				Section: "section-nums",
				Options: map[string]interface{}{
					"Int":     int(42),
					"Int8":    int8(42),
					"Int16":   int16(42),
					"Int32":   int32(42),
					"Int64":   int64(42),
					"Uint":    uint(42),
					"Uint8":   uint8(42),
					"Uint16":  uint16(42),
					"Uint32":  uint32(42),
					"Uint64":  uint64(42),
					"Float32": float32(42),
					"Float64": float64(42),
				},
			},
			exp: SectionNums{
				Int:     int(42),
				Int8:    int8(42),
				Int16:   int16(42),
				Int32:   int32(42),
				Int64:   int64(42),
				Uint:    uint(42),
				Uint8:   uint8(42),
				Uint16:  uint16(42),
				Uint32:  uint32(42),
				Uint64:  uint64(42),
				Float32: float32(42),
				Float64: float64(42),
			},
			redacted: map[string]interface{}{
				"Int":     int(42),
				"Int8":    int8(42),
				"Int16":   int16(42),
				"Int32":   int32(42),
				"Int64":   int64(42),
				"Uint":    uint(42),
				"Uint8":   uint8(42),
				"Uint16":  uint16(42),
				"Uint32":  uint32(42),
				"Uint64":  uint64(42),
				"Float32": float32(42),
				"Float64": float64(42),
			},
		},
		{
			o: override.Override{
				Section: "section-nums",
				Options: map[string]interface{}{
					"Int":     int(42),
					"Int8":    int(42),
					"Int16":   int(42),
					"Int32":   int(42),
					"Int64":   int(42),
					"Uint":    int(42),
					"Uint8":   int(42),
					"Uint16":  int(42),
					"Uint32":  int(42),
					"Uint64":  int(42),
					"Float32": int(42),
					"Float64": int(42),
				},
			},
			exp: SectionNums{
				Int:     int(42),
				Int8:    int8(42),
				Int16:   int16(42),
				Int32:   int32(42),
				Int64:   int64(42),
				Uint:    uint(42),
				Uint8:   uint8(42),
				Uint16:  uint16(42),
				Uint32:  uint32(42),
				Uint64:  uint64(42),
				Float32: float32(42),
				Float64: float64(42),
			},
			redacted: map[string]interface{}{
				"Int":     int(42),
				"Int8":    int8(42),
				"Int16":   int16(42),
				"Int32":   int32(42),
				"Int64":   int64(42),
				"Uint":    uint(42),
				"Uint8":   uint8(42),
				"Uint16":  uint16(42),
				"Uint32":  uint32(42),
				"Uint64":  uint64(42),
				"Float32": float32(42),
				"Float64": float64(42),
			},
		},
		{
			o: override.Override{
				Section: "section-nums",
				Options: map[string]interface{}{
					"Int":     int(42),
					"Int8":    int(42),
					"Int16":   int(42),
					"Int32":   int(42),
					"Int64":   int(42),
					"Uint":    int(42),
					"Uint8":   int(42),
					"Uint16":  int(42),
					"Uint32":  int(42),
					"Uint64":  int(42),
					"Float32": int(42),
					"Float64": int(42),
				},
			},
			exp: SectionNums{
				Int:     int(42),
				Int8:    int8(42),
				Int16:   int16(42),
				Int32:   int32(42),
				Int64:   int64(42),
				Uint:    uint(42),
				Uint8:   uint8(42),
				Uint16:  uint16(42),
				Uint32:  uint32(42),
				Uint64:  uint64(42),
				Float32: float32(42),
				Float64: float64(42),
			},
			redacted: map[string]interface{}{
				"Int":     int(42),
				"Int8":    int8(42),
				"Int16":   int16(42),
				"Int32":   int32(42),
				"Int64":   int64(42),
				"Uint":    uint(42),
				"Uint8":   uint8(42),
				"Uint16":  uint16(42),
				"Uint32":  uint32(42),
				"Uint64":  uint64(42),
				"Float32": float32(42),
				"Float64": float64(42),
			},
		},
		{
			o: override.Override{
				Section: "section-nums",
				Options: map[string]interface{}{
					"Int":     int8(42),
					"Int8":    int8(42),
					"Int16":   int8(42),
					"Int32":   int8(42),
					"Int64":   int8(42),
					"Uint":    int8(42),
					"Uint8":   int8(42),
					"Uint16":  int8(42),
					"Uint32":  int8(42),
					"Uint64":  int8(42),
					"Float32": int8(42),
					"Float64": int8(42),
				},
			},
			exp: SectionNums{
				Int:     int(42),
				Int8:    int8(42),
				Int16:   int16(42),
				Int32:   int32(42),
				Int64:   int64(42),
				Uint:    uint(42),
				Uint8:   uint8(42),
				Uint16:  uint16(42),
				Uint32:  uint32(42),
				Uint64:  uint64(42),
				Float32: float32(42),
				Float64: float64(42),
			},
			redacted: map[string]interface{}{
				"Int":     int(42),
				"Int8":    int8(42),
				"Int16":   int16(42),
				"Int32":   int32(42),
				"Int64":   int64(42),
				"Uint":    uint(42),
				"Uint8":   uint8(42),
				"Uint16":  uint16(42),
				"Uint32":  uint32(42),
				"Uint64":  uint64(42),
				"Float32": float32(42),
				"Float64": float64(42),
			},
		},
		{
			o: override.Override{
				Section: "section-nums",
				Options: map[string]interface{}{
					"Int":     int16(42),
					"Int8":    int16(42),
					"Int16":   int16(42),
					"Int32":   int16(42),
					"Int64":   int16(42),
					"Uint":    int16(42),
					"Uint8":   int16(42),
					"Uint16":  int16(42),
					"Uint32":  int16(42),
					"Uint64":  int16(42),
					"Float32": int16(42),
					"Float64": int16(42),
				},
			},
			exp: SectionNums{
				Int:     int(42),
				Int8:    int8(42),
				Int16:   int16(42),
				Int32:   int32(42),
				Int64:   int64(42),
				Uint:    uint(42),
				Uint8:   uint8(42),
				Uint16:  uint16(42),
				Uint32:  uint32(42),
				Uint64:  uint64(42),
				Float32: float32(42),
				Float64: float64(42),
			},
			redacted: map[string]interface{}{
				"Int":     int(42),
				"Int8":    int8(42),
				"Int16":   int16(42),
				"Int32":   int32(42),
				"Int64":   int64(42),
				"Uint":    uint(42),
				"Uint8":   uint8(42),
				"Uint16":  uint16(42),
				"Uint32":  uint32(42),
				"Uint64":  uint64(42),
				"Float32": float32(42),
				"Float64": float64(42),
			},
		},
		{
			o: override.Override{
				Section: "section-nums",
				Options: map[string]interface{}{
					"Int":     int32(42),
					"Int8":    int32(42),
					"Int16":   int32(42),
					"Int32":   int32(42),
					"Int64":   int32(42),
					"Uint":    int32(42),
					"Uint8":   int32(42),
					"Uint16":  int32(42),
					"Uint32":  int32(42),
					"Uint64":  int32(42),
					"Float32": int32(42),
					"Float64": int32(42),
				},
			},
			exp: SectionNums{
				Int:     int(42),
				Int8:    int8(42),
				Int16:   int16(42),
				Int32:   int32(42),
				Int64:   int64(42),
				Uint:    uint(42),
				Uint8:   uint8(42),
				Uint16:  uint16(42),
				Uint32:  uint32(42),
				Uint64:  uint64(42),
				Float32: float32(42),
				Float64: float64(42),
			},
			redacted: map[string]interface{}{
				"Int":     int(42),
				"Int8":    int8(42),
				"Int16":   int16(42),
				"Int32":   int32(42),
				"Int64":   int64(42),
				"Uint":    uint(42),
				"Uint8":   uint8(42),
				"Uint16":  uint16(42),
				"Uint32":  uint32(42),
				"Uint64":  uint64(42),
				"Float32": float32(42),
				"Float64": float64(42),
			},
		},
		{
			o: override.Override{
				Section: "section-nums",
				Options: map[string]interface{}{
					"Int":     int64(42),
					"Int8":    int64(42),
					"Int16":   int64(42),
					"Int32":   int64(42),
					"Int64":   int64(42),
					"Uint":    int64(42),
					"Uint8":   int64(42),
					"Uint16":  int64(42),
					"Uint32":  int64(42),
					"Uint64":  int64(42),
					"Float32": int64(42),
					"Float64": int64(42),
				},
			},
			exp: SectionNums{
				Int:     int(42),
				Int8:    int8(42),
				Int16:   int16(42),
				Int32:   int32(42),
				Int64:   int64(42),
				Uint:    uint(42),
				Uint8:   uint8(42),
				Uint16:  uint16(42),
				Uint32:  uint32(42),
				Uint64:  uint64(42),
				Float32: float32(42),
				Float64: float64(42),
			},
			redacted: map[string]interface{}{
				"Int":     int(42),
				"Int8":    int8(42),
				"Int16":   int16(42),
				"Int32":   int32(42),
				"Int64":   int64(42),
				"Uint":    uint(42),
				"Uint8":   uint8(42),
				"Uint16":  uint16(42),
				"Uint32":  uint32(42),
				"Uint64":  uint64(42),
				"Float32": float32(42),
				"Float64": float64(42),
			},
		},
		{
			o: override.Override{
				Section: "section-nums",
				Options: map[string]interface{}{
					"Int":     uint(42),
					"Int8":    uint(42),
					"Int16":   uint(42),
					"Int32":   uint(42),
					"Int64":   uint(42),
					"Uint":    uint(42),
					"Uint8":   uint(42),
					"Uint16":  uint(42),
					"Uint32":  uint(42),
					"Uint64":  uint(42),
					"Float32": uint(42),
					"Float64": uint(42),
				},
			},
			exp: SectionNums{
				Int:     int(42),
				Int8:    int8(42),
				Int16:   int16(42),
				Int32:   int32(42),
				Int64:   int64(42),
				Uint:    uint(42),
				Uint8:   uint8(42),
				Uint16:  uint16(42),
				Uint32:  uint32(42),
				Uint64:  uint64(42),
				Float32: float32(42),
				Float64: float64(42),
			},
			redacted: map[string]interface{}{
				"Int":     int(42),
				"Int8":    int8(42),
				"Int16":   int16(42),
				"Int32":   int32(42),
				"Int64":   int64(42),
				"Uint":    uint(42),
				"Uint8":   uint8(42),
				"Uint16":  uint16(42),
				"Uint32":  uint32(42),
				"Uint64":  uint64(42),
				"Float32": float32(42),
				"Float64": float64(42),
			},
		},
		{
			o: override.Override{
				Section: "section-nums",
				Options: map[string]interface{}{
					"Int":     uint8(42),
					"Int8":    uint8(42),
					"Int16":   uint8(42),
					"Int32":   uint8(42),
					"Int64":   uint8(42),
					"Uint":    uint8(42),
					"Uint8":   uint8(42),
					"Uint16":  uint8(42),
					"Uint32":  uint8(42),
					"Uint64":  uint8(42),
					"Float32": uint8(42),
					"Float64": uint8(42),
				},
			},
			exp: SectionNums{
				Int:     int(42),
				Int8:    int8(42),
				Int16:   int16(42),
				Int32:   int32(42),
				Int64:   int64(42),
				Uint:    uint(42),
				Uint8:   uint8(42),
				Uint16:  uint16(42),
				Uint32:  uint32(42),
				Uint64:  uint64(42),
				Float32: float32(42),
				Float64: float64(42),
			},
			redacted: map[string]interface{}{
				"Int":     int(42),
				"Int8":    int8(42),
				"Int16":   int16(42),
				"Int32":   int32(42),
				"Int64":   int64(42),
				"Uint":    uint(42),
				"Uint8":   uint8(42),
				"Uint16":  uint16(42),
				"Uint32":  uint32(42),
				"Uint64":  uint64(42),
				"Float32": float32(42),
				"Float64": float64(42),
			},
		},
		{
			o: override.Override{
				Section: "section-nums",
				Options: map[string]interface{}{
					"Int":     uint16(42),
					"Int8":    uint16(42),
					"Int16":   uint16(42),
					"Int32":   uint16(42),
					"Int64":   uint16(42),
					"Uint":    uint16(42),
					"Uint8":   uint16(42),
					"Uint16":  uint16(42),
					"Uint32":  uint16(42),
					"Uint64":  uint16(42),
					"Float32": uint16(42),
					"Float64": uint16(42),
				},
			},
			exp: SectionNums{
				Int:     int(42),
				Int8:    int8(42),
				Int16:   int16(42),
				Int32:   int32(42),
				Int64:   int64(42),
				Uint:    uint(42),
				Uint8:   uint8(42),
				Uint16:  uint16(42),
				Uint32:  uint32(42),
				Uint64:  uint64(42),
				Float32: float32(42),
				Float64: float64(42),
			},
			redacted: map[string]interface{}{
				"Int":     int(42),
				"Int8":    int8(42),
				"Int16":   int16(42),
				"Int32":   int32(42),
				"Int64":   int64(42),
				"Uint":    uint(42),
				"Uint8":   uint8(42),
				"Uint16":  uint16(42),
				"Uint32":  uint32(42),
				"Uint64":  uint64(42),
				"Float32": float32(42),
				"Float64": float64(42),
			},
		},
		{
			o: override.Override{
				Section: "section-nums",
				Options: map[string]interface{}{
					"Int":     uint32(42),
					"Int8":    uint32(42),
					"Int16":   uint32(42),
					"Int32":   uint32(42),
					"Int64":   uint32(42),
					"Uint":    uint32(42),
					"Uint8":   uint32(42),
					"Uint16":  uint32(42),
					"Uint32":  uint32(42),
					"Uint64":  uint32(42),
					"Float32": uint32(42),
					"Float64": uint32(42),
				},
			},
			exp: SectionNums{
				Int:     int(42),
				Int8:    int8(42),
				Int16:   int16(42),
				Int32:   int32(42),
				Int64:   int64(42),
				Uint:    uint(42),
				Uint8:   uint8(42),
				Uint16:  uint16(42),
				Uint32:  uint32(42),
				Uint64:  uint64(42),
				Float32: float32(42),
				Float64: float64(42),
			},
			redacted: map[string]interface{}{
				"Int":     int(42),
				"Int8":    int8(42),
				"Int16":   int16(42),
				"Int32":   int32(42),
				"Int64":   int64(42),
				"Uint":    uint(42),
				"Uint8":   uint8(42),
				"Uint16":  uint16(42),
				"Uint32":  uint32(42),
				"Uint64":  uint64(42),
				"Float32": float32(42),
				"Float64": float64(42),
			},
		},
		{
			o: override.Override{
				Section: "section-nums",
				Options: map[string]interface{}{
					"Int":     uint64(42),
					"Int8":    uint64(42),
					"Int16":   uint64(42),
					"Int32":   uint64(42),
					"Int64":   uint64(42),
					"Uint":    uint64(42),
					"Uint8":   uint64(42),
					"Uint16":  uint64(42),
					"Uint32":  uint64(42),
					"Uint64":  uint64(42),
					"Float32": uint64(42),
					"Float64": uint64(42),
				},
			},
			exp: SectionNums{
				Int:     int(42),
				Int8:    int8(42),
				Int16:   int16(42),
				Int32:   int32(42),
				Int64:   int64(42),
				Uint:    uint(42),
				Uint8:   uint8(42),
				Uint16:  uint16(42),
				Uint32:  uint32(42),
				Uint64:  uint64(42),
				Float32: float32(42),
				Float64: float64(42),
			},
			redacted: map[string]interface{}{
				"Int":     int(42),
				"Int8":    int8(42),
				"Int16":   int16(42),
				"Int32":   int32(42),
				"Int64":   int64(42),
				"Uint":    uint(42),
				"Uint8":   uint8(42),
				"Uint16":  uint16(42),
				"Uint32":  uint32(42),
				"Uint64":  uint64(42),
				"Float32": float32(42),
				"Float64": float64(42),
			},
		},
		{
			o: override.Override{
				Section: "section-nums",
				Options: map[string]interface{}{
					"Int":     float32(42),
					"Int8":    float32(42),
					"Int16":   float32(42),
					"Int32":   float32(42),
					"Int64":   float32(42),
					"Uint":    float32(42),
					"Uint8":   float32(42),
					"Uint16":  float32(42),
					"Uint32":  float32(42),
					"Uint64":  float32(42),
					"Float32": float32(42),
					"Float64": float32(42),
				},
			},
			exp: SectionNums{
				Int:     int(42),
				Int8:    int8(42),
				Int16:   int16(42),
				Int32:   int32(42),
				Int64:   int64(42),
				Uint:    uint(42),
				Uint8:   uint8(42),
				Uint16:  uint16(42),
				Uint32:  uint32(42),
				Uint64:  uint64(42),
				Float32: float32(42),
				Float64: float64(42),
			},
			redacted: map[string]interface{}{
				"Int":     int(42),
				"Int8":    int8(42),
				"Int16":   int16(42),
				"Int32":   int32(42),
				"Int64":   int64(42),
				"Uint":    uint(42),
				"Uint8":   uint8(42),
				"Uint16":  uint16(42),
				"Uint32":  uint32(42),
				"Uint64":  uint64(42),
				"Float32": float32(42),
				"Float64": float64(42),
			},
		},
		{
			o: override.Override{
				Section: "section-nums",
				Options: map[string]interface{}{
					"Int":     float64(42),
					"Int8":    float64(42),
					"Int16":   float64(42),
					"Int32":   float64(42),
					"Int64":   float64(42),
					"Uint":    float64(42),
					"Uint8":   float64(42),
					"Uint16":  float64(42),
					"Uint32":  float64(42),
					"Uint64":  float64(42),
					"Float32": float64(42),
					"Float64": float64(42),
				},
			},
			exp: SectionNums{
				Int:     int(42),
				Int8:    int8(42),
				Int16:   int16(42),
				Int32:   int32(42),
				Int64:   int64(42),
				Uint:    uint(42),
				Uint8:   uint8(42),
				Uint16:  uint16(42),
				Uint32:  uint32(42),
				Uint64:  uint64(42),
				Float32: float32(42),
				Float64: float64(42),
			},
			redacted: map[string]interface{}{
				"Int":     int(42),
				"Int8":    int8(42),
				"Int16":   int16(42),
				"Int32":   int32(42),
				"Int64":   int64(42),
				"Uint":    uint(42),
				"Uint8":   uint8(42),
				"Uint16":  uint16(42),
				"Uint32":  uint32(42),
				"Uint64":  uint64(42),
				"Float32": float32(42),
				"Float64": float64(42),
			},
		},
		{
			o: override.Override{
				Section: "section-nums",
				Options: map[string]interface{}{
					"Int":     "42",
					"Int8":    "42",
					"Int16":   "42",
					"Int32":   "42",
					"Int64":   "42",
					"Uint":    "42",
					"Uint8":   "42",
					"Uint16":  "42",
					"Uint32":  "42",
					"Uint64":  "42",
					"Float32": "42",
					"Float64": "42",
				},
			},
			exp: SectionNums{
				Int:     int(42),
				Int8:    int8(42),
				Int16:   int16(42),
				Int32:   int32(42),
				Int64:   int64(42),
				Uint:    uint(42),
				Uint8:   uint8(42),
				Uint16:  uint16(42),
				Uint32:  uint32(42),
				Uint64:  uint64(42),
				Float32: float32(42),
				Float64: float64(42),
			},
			redacted: map[string]interface{}{
				"Int":     int(42),
				"Int8":    int8(42),
				"Int16":   int16(42),
				"Int32":   int32(42),
				"Int64":   int64(42),
				"Uint":    uint(42),
				"Uint8":   uint8(42),
				"Uint16":  uint16(42),
				"Uint32":  uint32(42),
				"Uint64":  uint64(42),
				"Float32": float32(42),
				"Float64": float64(42),
			},
		},
		{
			o: override.Override{
				Section: "section-c",
				Options: map[string]interface{}{
					"Option4":  42,
					"Password": "supersecret",
				},
			},
			exp: &SectionC{
				Option4:  int64(42),
				Password: "supersecret",
			},
			redacted: map[string]interface{}{
				"Option4":  int64(42),
				"Password": true,
			},
		},
		{
			o: override.Override{
				Section: "section-d",
				Element: "x",
				Options: map[string]interface{}{
					"Option5": "x-new-5",
				},
			},
			exp: SectionD{
				ID:      "x",
				Option5: "x-new-5",
			},
			redacted: map[string]interface{}{
				"ID":      "x",
				"Option5": "x-new-5",
				"Option6": map[string]map[string]int(nil),
				"Option7": [][]int(nil),
			},
		},
		{
			o: override.Override{
				Section: "section-d",
				Element: "x",
				Options: map[string]interface{}{
					"Option6": map[string]interface{}{"a": map[string]interface{}{"b": 42}},
				},
			},
			exp: SectionD{
				ID:      "x",
				Option5: "x-5",
				Option6: map[string]map[string]int{"a": {"b": 42}},
			},
			redacted: map[string]interface{}{
				"ID":      "x",
				"Option5": "x-5",
				"Option6": map[string]map[string]int{"a": {"b": 42}},
				"Option7": [][]int(nil),
			},
		},
		{
			o: override.Override{
				Section: "section-d",
				Element: "x",
				Options: map[string]interface{}{
					"Option7": []interface{}{[]interface{}{6, 7, 42}, []interface{}{6, 9, 42}},
				},
			},
			exp: SectionD{
				ID:      "x",
				Option5: "x-5",
				Option7: [][]int{{6, 7, 42}, {6, 9, 42}},
			},
			redacted: map[string]interface{}{
				"ID":      "x",
				"Option5": "x-5",
				"Option6": map[string]map[string]int(nil),
				"Option7": [][]int{{6, 7, 42}, {6, 9, 42}},
			},
		},
		{
			// Test that a Stringer can convert into a number
			o: override.Override{
				Section: "section-d",
				Element: "x",
				Options: map[string]interface{}{
					"Option7": []interface{}{
						[]interface{}{
							json.Number("6"),
							json.Number("7"),
							json.Number("42"),
						},
						[]interface{}{
							json.Number("6"),
							json.Number("9"),
							json.Number("42"),
						}},
				},
			},
			exp: SectionD{
				ID:      "x",
				Option5: "x-5",
				Option7: [][]int{{6, 7, 42}, {6, 9, 42}},
			},
			redacted: map[string]interface{}{
				"ID":      "x",
				"Option5": "x-5",
				"Option6": map[string]map[string]int(nil),
				"Option7": [][]int{{6, 7, 42}, {6, 9, 42}},
			},
		},
	}
	for _, tc := range testCases {
		cu := override.New(testConfig)
		if tc.optionNameFunc != nil {
			cu.OptionNameFunc = tc.optionNameFunc
		}
		if newConfig, err := cu.Override(tc.o); err != nil {
			t.Fatal(err)
		} else {
			// Validate value
			if got := newConfig.Value(); !reflect.DeepEqual(got, tc.exp) {
				t.Errorf("unexpected newConfig.Value result:\ngot\n%#v\nexp\n%#v\n", got, tc.exp)
			}
			// Validate redacted
			if got, err := newConfig.Redacted(); err != nil {
				t.Fatal(err)
			} else if !reflect.DeepEqual(got, tc.redacted) {
				t.Errorf("unexpected newConfig.Redacted result:\ngot\n%#v\nexp\n%#v\n", got, tc.redacted)
			}
		}
		// Validate original not modified
		if !reflect.DeepEqual(testConfig, copy) {
			t.Errorf("original configuration object was modified. got %v exp %v", testConfig, copy)
		}
	}
}

func TestOverrider_OverrideAll(t *testing.T) {
	testConfig := &TestConfig{
		SectionA: SectionA{
			Option1: "o1",
		},
		SectionC: &SectionC{
			Option4: -1,
		},
		SectionDs: []SectionD{
			{
				ID:      "x",
				Option5: "x-5",
			},
			{
				ID:      "y",
				Option5: "y-5",
			},
			{
				ID:      "z",
				Option5: "z-5",
			},
		},
	}
	copy, err := copystructure.Copy(testConfig)
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		name           string
		os             []override.Override
		optionNameFunc override.OptionNameFunc
		exp            map[string][]map[string]interface{}
	}{
		{
			name: "leave section-c default",
			os: []override.Override{
				{
					Section: "section-a",
					Options: map[string]interface{}{
						"Option1": "new-1",
					},
				},
				{
					Section: "section-b",
					Options: map[string]interface{}{
						"Option3": "new-3",
					},
				},
				{
					Section: "section-d",
					Element: "y",
					Options: map[string]interface{}{
						"Option5": "y-new-5",
					},
				},
				{
					Section: "section-d",
					Element: "x",
					Options: map[string]interface{}{
						"Option5": "x-new-5",
					},
				},
			},
			exp: map[string][]map[string]interface{}{
				"section-a": {{
					"Option1": "new-1",
					"Option2": "",
				}},
				"section-b": {{
					"Option3": "new-3",
				}},
				"section-c": {{
					"Option4":  int64(-1),
					"Password": false,
				}},
				"section-nums": {{
					"Int":     int(0),
					"Int8":    int8(0),
					"Int16":   int16(0),
					"Int32":   int32(0),
					"Int64":   int64(0),
					"Uint":    uint(0),
					"Uint8":   uint8(0),
					"Uint16":  uint16(0),
					"Uint32":  uint32(0),
					"Uint64":  uint64(0),
					"Float32": float32(0),
					"Float64": float64(0),
				}},
				"section-d": {
					{
						"ID":      "x",
						"Option5": "x-new-5",
						"Option6": map[string]map[string]int(nil),
						"Option7": [][]int(nil),
					},
					{
						"ID":      "y",
						"Option5": "y-new-5",
						"Option6": map[string]map[string]int(nil),
						"Option7": [][]int(nil),
					},
					{
						"ID":      "z",
						"Option5": "z-5",
						"Option6": map[string]map[string]int(nil),
						"Option7": [][]int(nil),
					},
				},
			},
		},
		{
			name: "override section-c password create new section-d element",
			os: []override.Override{
				{
					Section: "section-c",
					Options: map[string]interface{}{
						"Password": "secret",
					},
				},
				{
					Section: "section-d",
					Create:  true,
					Options: map[string]interface{}{
						"ID":      "w",
						"Option5": "w-new-5",
					},
				},
			},
			exp: map[string][]map[string]interface{}{
				"section-a": {{
					"Option1": "o1",
					"Option2": "",
				}},
				"section-b": {{
					"Option3": "",
				}},
				"section-c": {{
					"Option4":  int64(-1),
					"Password": true,
				}},
				"section-nums": {{
					"Int":     int(0),
					"Int8":    int8(0),
					"Int16":   int16(0),
					"Int32":   int32(0),
					"Int64":   int64(0),
					"Uint":    uint(0),
					"Uint8":   uint8(0),
					"Uint16":  uint16(0),
					"Uint32":  uint32(0),
					"Uint64":  uint64(0),
					"Float32": float32(0),
					"Float64": float64(0),
				}},
				"section-d": {
					{
						"ID":      "w",
						"Option5": "w-new-5",
						"Option6": map[string]map[string]int(nil),
						"Option7": [][]int(nil),
					},
					{
						"ID":      "x",
						"Option5": "x-5",
						"Option6": map[string]map[string]int(nil),
						"Option7": [][]int(nil),
					},
					{
						"ID":      "y",
						"Option5": "y-5",
						"Option6": map[string]map[string]int(nil),
						"Option7": [][]int(nil),
					},
					{
						"ID":      "z",
						"Option5": "z-5",
						"Option6": map[string]map[string]int(nil),
						"Option7": [][]int(nil),
					},
				},
			},
		},
		{
			name: "delete element from section-d",
			os: []override.Override{
				{
					Section: "section-d",
					Element: "y",
					Delete:  true,
				},
			},
			exp: map[string][]map[string]interface{}{
				"section-a": {{
					"Option1": "o1",
					"Option2": "",
				}},
				"section-b": {{
					"Option3": "",
				}},
				"section-c": {{
					"Option4":  int64(-1),
					"Password": false,
				}},
				"section-nums": {{
					"Int":     int(0),
					"Int8":    int8(0),
					"Int16":   int16(0),
					"Int32":   int32(0),
					"Int64":   int64(0),
					"Uint":    uint(0),
					"Uint8":   uint8(0),
					"Uint16":  uint16(0),
					"Uint32":  uint32(0),
					"Uint64":  uint64(0),
					"Float32": float32(0),
					"Float64": float64(0),
				}},
				"section-d": {
					{
						"ID":      "x",
						"Option5": "x-5",
						"Option6": map[string]map[string]int(nil),
						"Option7": [][]int(nil),
					},
					{
						"ID":      "z",
						"Option5": "z-5",
						"Option6": map[string]map[string]int(nil),
						"Option7": [][]int(nil),
					},
				},
			},
		},
		{
			name: "create element in section-d, delete y from section-d",
			os: []override.Override{
				{
					Section: "section-d",
					Create:  true,
					Options: map[string]interface{}{
						"ID":      "w",
						"Option5": "w-new-5",
					},
				},
				{
					Section: "section-d",
					Element: "y",
					Delete:  true,
				},
			},
			exp: map[string][]map[string]interface{}{
				"section-a": {{
					"Option1": "o1",
					"Option2": "",
				}},
				"section-b": {{
					"Option3": "",
				}},
				"section-c": {{
					"Option4":  int64(-1),
					"Password": false,
				}},
				"section-nums": {{
					"Int":     int(0),
					"Int8":    int8(0),
					"Int16":   int16(0),
					"Int32":   int32(0),
					"Int64":   int64(0),
					"Uint":    uint(0),
					"Uint8":   uint8(0),
					"Uint16":  uint16(0),
					"Uint32":  uint32(0),
					"Uint64":  uint64(0),
					"Float32": float32(0),
					"Float64": float64(0),
				}},
				"section-d": {
					{
						"ID":      "w",
						"Option5": "w-new-5",
						"Option6": map[string]map[string]int(nil),
						"Option7": [][]int(nil),
					},
					{
						"ID":      "x",
						"Option5": "x-5",
						"Option6": map[string]map[string]int(nil),
						"Option7": [][]int(nil),
					},
					{
						"ID":      "z",
						"Option5": "z-5",
						"Option6": map[string]map[string]int(nil),
						"Option7": [][]int(nil),
					},
				},
			},
		},
		{
			name: "created element in section-d using defaults",
			os: []override.Override{
				{
					Section: "section-d",
					Create:  true,
					Options: map[string]interface{}{
						"ID": "w",
					},
				},
			},
			exp: map[string][]map[string]interface{}{
				"section-a": {{
					"Option1": "o1",
					"Option2": "",
				}},
				"section-b": {{
					"Option3": "",
				}},
				"section-c": {{
					"Option4":  int64(-1),
					"Password": false,
				}},
				"section-nums": {{
					"Int":     int(0),
					"Int8":    int8(0),
					"Int16":   int16(0),
					"Int32":   int32(0),
					"Int64":   int64(0),
					"Uint":    uint(0),
					"Uint8":   uint8(0),
					"Uint16":  uint16(0),
					"Uint32":  uint32(0),
					"Uint64":  uint64(0),
					"Float32": float32(0),
					"Float64": float64(0),
				}},
				"section-d": {
					{
						"ID":      "w",
						"Option5": "o5",
						"Option6": map[string]map[string]int(nil),
						"Option7": [][]int(nil),
					},
					{
						"ID":      "x",
						"Option5": "x-5",
						"Option6": map[string]map[string]int(nil),
						"Option7": [][]int(nil),
					},
					{
						"ID":      "y",
						"Option5": "y-5",
						"Option6": map[string]map[string]int(nil),
						"Option7": [][]int(nil),
					},
					{
						"ID":      "z",
						"Option5": "z-5",
						"Option6": map[string]map[string]int(nil),
						"Option7": [][]int(nil),
					},
				},
			},
		},
		{
			name: "delete created element in section-d",
			os: []override.Override{
				{
					Section: "section-d",
					Create:  true,
					Options: map[string]interface{}{
						"ID":      "w",
						"Option5": "w-new-5",
					},
				},
				{
					Section: "section-d",
					Element: "y",
					Delete:  true,
				},
				{
					Section: "section-d",
					Element: "w",
					Delete:  true,
				},
			},
			exp: map[string][]map[string]interface{}{
				"section-a": {{
					"Option1": "o1",
					"Option2": "",
				}},
				"section-b": {{
					"Option3": "",
				}},
				"section-c": {{
					"Option4":  int64(-1),
					"Password": false,
				}},
				"section-nums": {{
					"Int":     int(0),
					"Int8":    int8(0),
					"Int16":   int16(0),
					"Int32":   int32(0),
					"Int64":   int64(0),
					"Uint":    uint(0),
					"Uint8":   uint8(0),
					"Uint16":  uint16(0),
					"Uint32":  uint32(0),
					"Uint64":  uint64(0),
					"Float32": float32(0),
					"Float64": float64(0),
				}},
				"section-d": {
					{
						"ID":      "x",
						"Option5": "x-5",
						"Option6": map[string]map[string]int(nil),
						"Option7": [][]int(nil),
					},
					{
						"ID":      "z",
						"Option5": "z-5",
						"Option6": map[string]map[string]int(nil),
						"Option7": [][]int(nil),
					},
				},
			},
		},
		{
			name: "delete all elements in section-d",
			os: []override.Override{
				{
					Section: "section-d",
					Element: "x",
					Delete:  true,
				},
				{
					Section: "section-d",
					Element: "y",
					Delete:  true,
				},
				{
					Section: "section-d",
					Element: "z",
					Delete:  true,
				},
			},
			exp: map[string][]map[string]interface{}{
				"section-a": {{
					"Option1": "o1",
					"Option2": "",
				}},
				"section-b": {{
					"Option3": "",
				}},
				"section-c": {{
					"Option4":  int64(-1),
					"Password": false,
				}},
				"section-nums": {{
					"Int":     int(0),
					"Int8":    int8(0),
					"Int16":   int16(0),
					"Int32":   int32(0),
					"Int64":   int64(0),
					"Uint":    uint(0),
					"Uint8":   uint8(0),
					"Uint16":  uint16(0),
					"Uint32":  uint32(0),
					"Uint64":  uint64(0),
					"Float32": float32(0),
					"Float64": float64(0),
				}},
			},
		},
	}

	for _, tc := range testCases {
		t.Log(tc.name)
		cu := override.New(testConfig)
		if tc.optionNameFunc != nil {
			cu.OptionNameFunc = tc.optionNameFunc
		}
		if sections, err := cu.OverrideAll(tc.os); err != nil {
			t.Fatal(err)
		} else {
			// Validate sections
			if got, exp := len(sections), len(tc.exp); got != exp {
				t.Errorf("unexpected section count got %d exp %d", got, exp)
				continue
			}
			for name, sectionList := range sections {
				expSectionList, ok := tc.exp[name]
				if !ok {
					t.Errorf("extra section returned %s", name)
					break
				}
				if got, exp := len(sectionList), len(expSectionList); got != exp {
					t.Errorf("unexpected section list count got %v exp %v", sectionList, expSectionList)
					break
				}
				for i, s := range sectionList {
					redacted, err := s.Redacted()
					if err != nil {
						t.Fatal(err)
					}
					if got, exp := redacted, expSectionList[i]; !reflect.DeepEqual(got, exp) {
						t.Errorf("unexpected sections result for %s element %d: \ngot\n%v\nexp\n%v\n", name, i, got, exp)
					}
				}
			}
		}
		// Validate original not modified
		if !reflect.DeepEqual(testConfig, copy) {
			t.Errorf("original configuration object was modified. got %v exp %v", testConfig, copy)
		}
	}
}
