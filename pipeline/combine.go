package pipeline

import (
	"fmt"
	"strings"
	"time"

	"github.com/influxdata/kapacitor/tick/ast"
)

const (
	defaultCombineDelimiter = "."
	defaultMaxCombinations  = 1e6
)

// Combine the data from a single node with itself.
// Points with the same time are grouped and then combinations are created.
// The size of the combinations is defined by how many expressions are given.
// Combinations are order independent and will not ever include the same point multiple times.
//
// Example:
//    stream
//        |from()
//            .measurement('request_latency')
//        |combine(lambda: "service" == 'login', lambda: TRUE)
//            .as('login', 'other')
//            // points that are within 1 second are considered the same time.
//            .tolerance(1s)
//            // delimiter for new field and tag names
//            .delimiter('.')
//        // Change group by to be new other.service tag
//        |groupBy('other.service')
//        // Both the "value" fields from each data point have been prefixed
//        // with the respective names 'login' and 'other'.
//        |eval(lambda: "login.value" / "other.value")
//           .as('ratio')
//        ...
//
// In the above example the data points for the `login` service are combined with the data points from all other services.
//
// Example:
//        |combine(lambda: TRUE, lambda: TRUE)
//            .as('login', 'other')
//
// In the above example all combination pairs are created.
//
// Example:
//        |combine(lambda: TRUE, lambda: TRUE, lambda: TRUE)
//            .as('login', 'other', 'another')
//
// In the above example all combinations triples are created.
type CombineNode struct {
	chainnode

	// The list of expressions for matching pairs
	// tick:ignore
	Lambdas []*ast.LambdaNode

	// The alias names of the two parents.
	// Note:
	//       Names[1] corresponds to the left  parent
	//       Names[0] corresponds to the right parent
	// tick:ignore
	Names []string `tick:"As"`

	// The delimiter between the As names and existing field an tag keys.
	// Can be the empty string, but you are responsible for ensuring conflicts are not possible if you use the empty string.
	Delimiter string

	// The maximum duration of time that two incoming points
	// can be apart and still be considered to be equal in time.
	// The joined data point's time will be rounded to the nearest
	// multiple of the tolerance duration.
	Tolerance time.Duration

	// Maximum number of possible combinations.
	// Since the number of possible combinations can grow very rapidly
	// you can set a maximum number of combinations allowed.
	// If the max is crossed, an error is logged and the combinations are not calculated.
	// Default: 10,000
	Max int64
}

func newCombineNode(e EdgeType, lambdas []*ast.LambdaNode) *CombineNode {
	c := &CombineNode{
		chainnode: newBasicChainNode("combine", e, StreamEdge),
		Lambdas:   lambdas,
		Delimiter: defaultCombineDelimiter,
		Max:       defaultMaxCombinations,
	}
	return c
}

// Prefix names for all fields from the respective nodes.
// Each field from the parent nodes will be prefixed with the provided name and a '.'.
// See the example above.
//
// The names cannot have a dot '.' character.
//
// tick:property
func (n *CombineNode) As(names ...string) *CombineNode {
	n.Names = names
	return n
}

// Validate that the as() specification is consistent with the number of combine expressions.
func (n *CombineNode) validate() error {
	if len(n.Names) == 0 {
		return fmt.Errorf("a call to combine.as() is required to specify the output stream prefixes.")
	}

	if len(n.Names) != len(n.Lambdas) {
		return fmt.Errorf("number of prefixes specified by combine.as() must match the number of combine expressions")
	}

	for _, name := range n.Names {
		if len(name) == 0 {
			return fmt.Errorf("must provide a prefix name for the combine node, see .as() property method")
		}
		if strings.Contains(name, n.Delimiter) {
			return fmt.Errorf("cannot use name %s as field prefix, it contains the delimiter character %s", name, n.Delimiter)
		}
	}
	names := make(map[string]bool, len(n.Names))
	for _, name := range n.Names {
		if names[name] {
			return fmt.Errorf("cannot use the same prefix name see .as() property method")
		}
		names[name] = true
	}

	return nil
}
