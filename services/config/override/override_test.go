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
	Option1 string `override:"option1"`
	Option2 string `override:"option2"`
}
type SectionB struct {
	Option3 string `override:"option3"`
}
type SectionC struct {
	Option4  int64  `override:"option4"`
	Password string `override:"password,redact"`
}

type SectionD struct {
	ID      string                    `override:"id"`
	Option5 string                    `override:"option5"`
	Option6 map[string]map[string]int `override:"option6"`
	Option7 [][]int                   `override:"option7"`
}

func (d *SectionD) Init() {
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
	SectionDs      []SectionD  `override:"section-d,element-key=id"`
	SectionIgnored SectionIgnored
	IgnoredInt     int
	IgnoredString  string
}

func ExampleOverrideConfig() {
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

	// Override options in section-a
	if newConfig, err := override.OverrideConfig(config, []override.Override{
		{
			Section: "section-a",
			Options: map[string]interface{}{
				"option1": "new option1 value",
				"option2": "initial option2 value",
			},
		},
		{
			Section: "section-b",
			Options: map[string]interface{}{
				"option3": "initial option3 value",
			},
		},
		{
			Section: "section-c",
			Options: map[string]interface{}{
				"option4": 586,
			},
		},
		{
			Section: "section-d",
			Element: "x",
			Options: map[string]interface{}{
				"option5": "x-new-5",
			},
		},
		{
			Section: "section-d",
			Element: "y",
			Options: map[string]interface{}{
				"option5": "y-new-5",
			},
		},
		{
			Section: "section-d",
			Create:  true,
			Options: map[string]interface{}{
				"id":      "w",
				"option5": "w-new-5",
			},
		},
	}); err != nil {
		fmt.Println("ERROR:", err)
	} else {
		a := newConfig["section-a"][0].Value().(SectionA)
		fmt.Println("New SectionA.Option1:", a.Option1)
		fmt.Println("New SectionA.Option2:", a.Option2)

		b := newConfig["section-b"][0].Value().(SectionB)
		fmt.Println("New SectionB.Option3:", b.Option3)

		c := newConfig["section-c"][0].Value().(*SectionC)
		fmt.Println("New SectionC.Option4:", c.Option4)

		// NOTE: Section elements are sorted by element key
		d := newConfig["section-d"]
		d0 := d[0].Value().(SectionD)
		d1 := d[1].Value().(SectionD)
		d2 := d[2].Value().(SectionD)
		d3 := d[3].Value().(SectionD)

		fmt.Println("New SectionD[0].Option5:", d0.Option5)
		fmt.Println("New SectionD[1].Option5:", d1.Option5)
		fmt.Println("New SectionD[2].Option5:", d2.Option5)
		fmt.Println("Old SectionD[3].Option5:", d3.Option5)
	}

	//Output:
	// New SectionA.Option1: new option1 value
	// New SectionA.Option2: initial option2 value
	// New SectionB.Option3: initial option3 value
	// New SectionC.Option4: 586
	// New SectionD[0].Option5: w-new-5
	// New SectionD[1].Option5: x-new-5
	// New SectionD[2].Option5: y-new-5
	// Old SectionD[3].Option5: z-5
}

func TestOverrideConfig_Single(t *testing.T) {
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
		o            override.Override
		exp          interface{}
		redacted     map[string]interface{}
		redactedList []string
	}{
		{
			o: override.Override{
				Section: "section-a",
				Options: map[string]interface{}{
					"option1": "new-o1",
				},
			},
			exp: SectionA{
				Option1: "new-o1",
			},
			redacted: map[string]interface{}{
				"option1": "new-o1",
				"option2": "",
			},
		},
		{
			o: override.Override{
				Section: "section-a",
				Options: map[string]interface{}{
					"option1": "new-o1",
				},
			},
			exp: SectionA{
				Option1: "new-o1",
			},
			redacted: map[string]interface{}{
				"option1": "new-o1",
				"option2": "",
			},
		},
		{
			o: override.Override{
				Section: "section-c",
				Options: map[string]interface{}{
					"option4": 42,
				},
			},
			exp: &SectionC{
				Option4: 42,
			},
			redacted: map[string]interface{}{
				"option4":  int64(42),
				"password": false,
			},
			redactedList: []string{"password"},
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
					"option4":  42,
					"password": "supersecret",
				},
			},
			exp: &SectionC{
				Option4:  int64(42),
				Password: "supersecret",
			},
			redacted: map[string]interface{}{
				"option4":  int64(42),
				"password": true,
			},
			redactedList: []string{"password"},
		},
		{
			o: override.Override{
				Section: "section-d",
				Element: "x",
				Options: map[string]interface{}{
					"option5": "x-new-5",
				},
			},
			exp: SectionD{
				ID:      "x",
				Option5: "x-new-5",
			},
			redacted: map[string]interface{}{
				"id":      "x",
				"option5": "x-new-5",
				"option6": map[string]map[string]int(nil),
				"option7": [][]int(nil),
			},
		},
		{
			o: override.Override{
				Section: "section-d",
				Element: "x",
				Options: map[string]interface{}{
					"option6": map[string]interface{}{"a": map[string]interface{}{"b": 42}},
				},
			},
			exp: SectionD{
				ID:      "x",
				Option5: "x-5",
				Option6: map[string]map[string]int{"a": {"b": 42}},
			},
			redacted: map[string]interface{}{
				"id":      "x",
				"option5": "x-5",
				"option6": map[string]map[string]int{"a": {"b": 42}},
				"option7": [][]int(nil),
			},
		},
		{
			o: override.Override{
				Section: "section-d",
				Element: "x",
				Options: map[string]interface{}{
					"option7": []interface{}{[]interface{}{6, 7, 42}, []interface{}{6, 9, 42}},
				},
			},
			exp: SectionD{
				ID:      "x",
				Option5: "x-5",
				Option7: [][]int{{6, 7, 42}, {6, 9, 42}},
			},
			redacted: map[string]interface{}{
				"id":      "x",
				"option5": "x-5",
				"option6": map[string]map[string]int(nil),
				"option7": [][]int{{6, 7, 42}, {6, 9, 42}},
			},
		},
		{
			// Test that a Stringer can convert into a number
			o: override.Override{
				Section: "section-d",
				Element: "x",
				Options: map[string]interface{}{
					"option7": []interface{}{
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
				"id":      "x",
				"option5": "x-5",
				"option6": map[string]map[string]int(nil),
				"option7": [][]int{{6, 7, 42}, {6, 9, 42}},
			},
		},
	}
	for _, tc := range testCases {
		if newConfig, err := override.OverrideConfig(testConfig, []override.Override{tc.o}); err != nil {
			t.Fatal(err)
		} else {
			element := newConfig[tc.o.Section][0]
			// Validate value
			if got := element.Value(); !reflect.DeepEqual(got, tc.exp) {
				t.Errorf("unexpected newConfig.Value result:\ngot\n%#v\nexp\n%#v\n", got, tc.exp)
			}
			// Validate redacted
			if gotOptions, gotList, err := element.Redacted(); err != nil {
				t.Fatal(err)
			} else if !reflect.DeepEqual(gotOptions, tc.redacted) {
				t.Errorf("unexpected newConfig.Redacted Options result:\ngot\n%#v\nexp\n%#v\n", gotOptions, tc.redacted)
			} else if !reflect.DeepEqual(gotList, tc.redactedList) {
				t.Errorf("unexpected newConfig.Redacted List result:\ngot\n%#v\nexp\n%#v\n", gotList, tc.redactedList)
			}
		}
		// Validate original not modified
		if !reflect.DeepEqual(testConfig, copy) {
			t.Errorf("original configuration object was modified. got %v exp %v", testConfig, copy)
		}
	}
}

func TestOverrideConfig_Multiple(t *testing.T) {
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
		name string
		os   []override.Override
		exp  map[string][]map[string]interface{}
	}{
		{
			name: "leave section-c default",
			os: []override.Override{
				{
					Section: "section-a",
					Options: map[string]interface{}{
						"option1": "new-1",
					},
				},
				{
					Section: "section-b",
					Options: map[string]interface{}{
						"option3": "new-3",
					},
				},
				{
					Section: "section-d",
					Element: "y",
					Options: map[string]interface{}{
						"option5": "y-new-5",
					},
				},
				{
					Section: "section-d",
					Element: "x",
					Options: map[string]interface{}{
						"option5": "x-new-5",
					},
				},
			},
			exp: map[string][]map[string]interface{}{
				"section-a": {{
					"option1": "new-1",
					"option2": "",
				}},
				"section-b": {{
					"option3": "new-3",
				}},
				"section-c": {{
					"option4":  int64(-1),
					"password": false,
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
						"id":      "x",
						"option5": "x-new-5",
						"option6": map[string]map[string]int(nil),
						"option7": [][]int(nil),
					},
					{
						"id":      "y",
						"option5": "y-new-5",
						"option6": map[string]map[string]int(nil),
						"option7": [][]int(nil),
					},
					{
						"id":      "z",
						"option5": "z-5",
						"option6": map[string]map[string]int(nil),
						"option7": [][]int(nil),
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
						"password": "secret",
					},
				},
				{
					Section: "section-d",
					Create:  true,
					Options: map[string]interface{}{
						"id":      "w",
						"option5": "w-new-5",
					},
				},
			},
			exp: map[string][]map[string]interface{}{
				"section-a": {{
					"option1": "o1",
					"option2": "",
				}},
				"section-b": {{
					"option3": "",
				}},
				"section-c": {{
					"option4":  int64(-1),
					"password": true,
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
						"id":      "w",
						"option5": "w-new-5",
						"option6": map[string]map[string]int(nil),
						"option7": [][]int(nil),
					},
					{
						"id":      "x",
						"option5": "x-5",
						"option6": map[string]map[string]int(nil),
						"option7": [][]int(nil),
					},
					{
						"id":      "y",
						"option5": "y-5",
						"option6": map[string]map[string]int(nil),
						"option7": [][]int(nil),
					},
					{
						"id":      "z",
						"option5": "z-5",
						"option6": map[string]map[string]int(nil),
						"option7": [][]int(nil),
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
					"option1": "o1",
					"option2": "",
				}},
				"section-b": {{
					"option3": "",
				}},
				"section-c": {{
					"option4":  int64(-1),
					"password": false,
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
						"id":      "x",
						"option5": "x-5",
						"option6": map[string]map[string]int(nil),
						"option7": [][]int(nil),
					},
					{
						"id":      "z",
						"option5": "z-5",
						"option6": map[string]map[string]int(nil),
						"option7": [][]int(nil),
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
						"id":      "w",
						"option5": "w-new-5",
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
					"option1": "o1",
					"option2": "",
				}},
				"section-b": {{
					"option3": "",
				}},
				"section-c": {{
					"option4":  int64(-1),
					"password": false,
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
						"id":      "w",
						"option5": "w-new-5",
						"option6": map[string]map[string]int(nil),
						"option7": [][]int(nil),
					},
					{
						"id":      "x",
						"option5": "x-5",
						"option6": map[string]map[string]int(nil),
						"option7": [][]int(nil),
					},
					{
						"id":      "z",
						"option5": "z-5",
						"option6": map[string]map[string]int(nil),
						"option7": [][]int(nil),
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
						"id": "w",
					},
				},
			},
			exp: map[string][]map[string]interface{}{
				"section-a": {{
					"option1": "o1",
					"option2": "",
				}},
				"section-b": {{
					"option3": "",
				}},
				"section-c": {{
					"option4":  int64(-1),
					"password": false,
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
						"id":      "w",
						"option5": "o5",
						"option6": map[string]map[string]int(nil),
						"option7": [][]int(nil),
					},
					{
						"id":      "x",
						"option5": "x-5",
						"option6": map[string]map[string]int(nil),
						"option7": [][]int(nil),
					},
					{
						"id":      "y",
						"option5": "y-5",
						"option6": map[string]map[string]int(nil),
						"option7": [][]int(nil),
					},
					{
						"id":      "z",
						"option5": "z-5",
						"option6": map[string]map[string]int(nil),
						"option7": [][]int(nil),
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
						"id":      "w",
						"option5": "w-new-5",
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
					"option1": "o1",
					"option2": "",
				}},
				"section-b": {{
					"option3": "",
				}},
				"section-c": {{
					"option4":  int64(-1),
					"password": false,
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
						"id":      "x",
						"option5": "x-5",
						"option6": map[string]map[string]int(nil),
						"option7": [][]int(nil),
					},
					{
						"id":      "z",
						"option5": "z-5",
						"option6": map[string]map[string]int(nil),
						"option7": [][]int(nil),
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
					"option1": "o1",
					"option2": "",
				}},
				"section-b": {{
					"option3": "",
				}},
				"section-c": {{
					"option4":  int64(-1),
					"password": false,
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
		if sections, err := override.OverrideConfig(testConfig, tc.os); err != nil {
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
					redacted, _, err := s.Redacted()
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
