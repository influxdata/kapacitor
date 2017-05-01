/* Overrider provides an API for overriding and reading redacted values for a configuration object.
The configuration object provided is expected to have two levels of nested structs.
The top level struct should have fields called "sections".
These fields may either be a struct or a slice of structs.
As such a section consists of a list of elements.
In the case where the field is a struct and not a slice, the section list always contains one element.
Further nested levels may exist but Overrider will not interact with them directly.
If a nested field is a struct, then github.com/mitchellh/mapstructure will be used to decode the map into the struct.

In order for a section to be overridden an `override` struct tag must be present.
The `override` tag defines a name for the section and option.
Struct tags can be used to mark options as redacted by adding a `<name>,redact` to the end of the `override` tag value.



Example:
   type SectionAConfig struct {
       Option   string `override:"option"`
       Password string `override:"password,redact"`
   }

   type SectionBConfig struct {
       ID       string `override:"id"`
       Option   string `override:"option"`
   }

   type Config struct {
       SectionA       SectionAConfig   `override:"section-a"`
       SectionB       []SectionBConfig `override:"section-b,element-key=id"`
       IgnoredSection IgnoredConfig
       IgnoredField   string
   }

   type IgnoredConfig struct {
      // contains anything ...
   }

   // Setup
   c := Config{
       SectionA: SectionAConfig{
            Option:   "option value",
            Password: "secret",
       },
       SectionB: []SectionBConfig{
           {
               ID:     "id0",
               Option: "option value 0",
           },
           {
               ID:     "id1",
               Option: "option value 1",
           },
       },
       IgnoredSection: IgnoredConfig{},
       IgnoredField: "this value is ignored",
   }
   o := override.New(c)
   // Read redacted section values
   redacted, err := o.Sections()
   // Override options for a section
   newElement, err := o.Override(Override{
       Section: "section-b",
       Element: "id1", // Element may be empty when overriding a section which is not a list.
       Options: map[string]interface{}{"option": "overridden option value"},
   })
*/
package override
