package pipeline

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/kapacitor/tick/ast"
)

// A StreamNode represents the source of data being
// streamed to Kapacitor via any of its inputs.
// The `stream` variable in stream tasks is an instance of
// a StreamNode.
// StreamNode.From is the method/property of this node.
type StreamNode struct {
	node
}

func newStreamNode() *StreamNode {
	return &StreamNode{
		node: node{
			desc:     "stream",
			wants:    StreamEdge,
			provides: StreamEdge,
		},
	}
}

// MarshalJSON converts StreamNode to JSON
// tick:ignore
func (n *StreamNode) MarshalJSON() ([]byte, error) {
	type Alias StreamNode
	var raw = &struct {
		TypeOf
		*Alias
	}{
		TypeOf: TypeOf{
			Type: "stream",
			ID:   n.ID(),
		},
		Alias: (*Alias)(n),
	}
	return json.Marshal(raw)
}

// UnmarshalJSON converts JSON to an StreamNode
// tick:ignore
func (n *StreamNode) UnmarshalJSON(data []byte) error {
	type Alias StreamNode
	var raw = &struct {
		TypeOf
		*Alias
	}{
		Alias: (*Alias)(n),
	}
	err := json.Unmarshal(data, raw)
	if err != nil {
		return err
	}
	if raw.Type != "stream" {
		return fmt.Errorf("error unmarshaling node %d of type %s as StreamNode", raw.ID, raw.Type)
	}
	n.setID(raw.ID)
	return nil
}

// Creates a new FromNode that can be further
// filtered using the Database, RetentionPolicy, Measurement and Where properties.
// From can be called multiple times to create multiple
// independent forks of the data stream.
//
// Example:
//    // Select the 'cpu' measurement from just the database 'mydb'
//    // and retention policy 'myrp'.
//    var cpu = stream
//        |from()
//            .database('mydb')
//            .retentionPolicy('myrp')
//            .measurement('cpu')
//    // Select the 'load' measurement from any database and retention policy.
//    var load = stream
//        |from()
//            .measurement('load')
//    // Join cpu and load streams and do further processing.
//    cpu
//        |join(load)
//            .as('cpu', 'load')
//        ...
//
func (s *StreamNode) From() *FromNode {
	f := newFromNode()
	s.linkChild(f)
	return f
}

// A FromNode selects a subset of the data flowing through a StreamNode.
// The stream node allows you to select which portion of the stream you want to process.
//
// Example:
//    stream
//        |from()
//           .database('mydb')
//           .retentionPolicy('myrp')
//           .measurement('mymeasurement')
//           .where(lambda: "host" =~ /logger\d+/)
//        |window()
//        ...
//
// The above example selects only data points from the database `mydb`
// and retention policy `myrp` and measurement `mymeasurement` where
// the tag `host` matches the regex `logger\d+`
type FromNode struct {
	chainnode `json:"-"`

	// An expression to filter the data stream.
	// tick:ignore
	Lambda *ast.LambdaNode `tick:"Where" json:"where"`

	// The dimensions by which to group to the data.
	// tick:ignore
	Dimensions []interface{} `tick:"GroupBy" json:"groupBy"`

	// Whether to include the measurement in the group ID.
	// tick:ignore
	GroupByMeasurementFlag bool `tick:"GroupByMeasurement" json:"groupByMeasurement"`

	// The database name.
	// If empty any database will be used.
	Database string `json:"database"`

	// The retention policy name
	// If empty any retention policy will be used.
	RetentionPolicy string `json:"retentionPolicy"`

	// The measurement name
	// If empty any measurement will be used.
	Measurement string `json:"measurement"`

	// Optional duration for truncating timestamps.
	// Helpful to ensure data points land on specific boundaries
	// Example:
	//    stream
	//       |from()
	//           .measurement('mydata')
	//           .truncate(1s)
	//
	// All incoming data will be truncated to 1 second resolution.
	Truncate time.Duration `json:"truncate"`

	// Optional duration for rounding timestamps.
	// Helpful to ensure data points land on specific boundaries
	// Example:
	//    stream
	//       |from()
	//           .measurement('mydata')
	//           .round(1s)
	//
	// All incoming data will be rounded to the nearest 1 second boundary.
	Round time.Duration `json:"round"`
}

func newFromNode() *FromNode {
	return &FromNode{
		chainnode: newBasicChainNode("from", StreamEdge, StreamEdge),
	}
}

// MarshalJSON converts FromNode to JSON
// tick:ignore
func (n *FromNode) MarshalJSON() ([]byte, error) {
	type Alias FromNode
	var raw = &struct {
		TypeOf
		*Alias
		Round    string `json:"round"`
		Truncate string `json:"truncate"`
	}{
		TypeOf: TypeOf{
			Type: "from",
			ID:   n.ID(),
		},
		Alias:    (*Alias)(n),
		Round:    influxql.FormatDuration(n.Round),
		Truncate: influxql.FormatDuration(n.Truncate),
	}
	return json.Marshal(raw)
}

// UnmarshalJSON converts JSON to an FromNode
// tick:ignore
func (n *FromNode) UnmarshalJSON(data []byte) error {
	type Alias FromNode
	var raw = &struct {
		TypeOf
		*Alias
		Round    string `json:"round"`
		Truncate string `json:"truncate"`
	}{
		Alias: (*Alias)(n),
	}
	err := json.Unmarshal(data, raw)
	if err != nil {
		return err
	}
	if raw.Type != "from" {
		return fmt.Errorf("error unmarshaling node %d of type %s as FromNode", raw.ID, raw.Type)
	}

	n.Round, err = influxql.ParseDuration(raw.Round)
	if err != nil {
		return err
	}

	n.Truncate, err = influxql.ParseDuration(raw.Truncate)
	if err != nil {
		return err
	}

	n.setID(raw.ID)
	return nil
}

//tick:ignore
func (n *FromNode) ChainMethods() map[string]reflect.Value {
	return map[string]reflect.Value{
		"GroupBy": reflect.ValueOf(n.chainnode.GroupBy),
		"Where":   reflect.ValueOf(n.chainnode.Where),
	}
}

// Creates a new stream node that can be further
// filtered using the Database, RetentionPolicy, Measurement and Where properties.
// From can be called multiple times to create multiple
// independent forks of the data stream.
//
// Example:
//    // Select the 'cpu' measurement from just the database 'mydb'
//    // and retention policy 'myrp'.
//    var cpu = stream
//        |from()
//            .database('mydb')
//            .retentionPolicy('myrp')
//            .measurement('cpu')
//    // Select the 'load' measurement from any database and retention policy.
//    var load = stream
//        |from()
//            .measurement('load')
//    // Join cpu and load streams and do further processing.
//    cpu
//        |join(load)
//            .as('cpu', 'load')
//        ...
//
func (s *FromNode) From() *FromNode {
	f := newFromNode()
	s.linkChild(f)
	return f
}

// Filter the current stream using the given expression.
// This expression is a Kapacitor expression. Kapacitor
// expressions are a superset of InfluxQL WHERE expressions.
// See the [expression](https://docs.influxdata.com/kapacitor/latest/tick/expr/) docs for more information.
//
// Multiple calls to the Where method will `AND` together each expression.
//
// Example:
//    stream
//       |from()
//          .where(lambda: condition1)
//          .where(lambda: condition2)
//
// The above is equivalent to this
// Example:
//    stream
//       |from()
//          .where(lambda: condition1 AND condition2)
//
//
// NOTE: Becareful to always use `|from` if you want multiple different streams.
//
// Example:
//  var data = stream
//      |from()
//          .measurement('cpu')
//  var total = data
//      .where(lambda: "cpu" == 'cpu-total')
//  var others = data
//      .where(lambda: "cpu" != 'cpu-total')
//
// The example above is equivalent to the example below,
// which is obviously not what was intended.
//
// Example:
//  var data = stream
//      |from()
//          .measurement('cpu')
//          .where(lambda: "cpu" == 'cpu-total' AND "cpu" != 'cpu-total')
//  var total = data
//  var others = total
//
// The example below will create two different streams each selecting
// a different subset of the original stream.
//
// Example:
//  var data = stream
//      |from()
//          .measurement('cpu')
//  var total = stream
//      |from()
//          .measurement('cpu')
//          .where(lambda: "cpu" == 'cpu-total')
//  var others = stream
//      |from()
//          .measurement('cpu')
//          .where(lambda: "cpu" != 'cpu-total')
//
//
// If empty then all data points are considered to match.
// tick:property
func (s *FromNode) Where(lambda *ast.LambdaNode) *FromNode {
	if s.Lambda != nil {
		s.Lambda.Expression = &ast.BinaryNode{
			Operator: ast.TokenAnd,
			Left:     s.Lambda.Expression,
			Right:    lambda.Expression,
		}
	} else {
		s.Lambda = lambda
	}
	return s
}

// Group the data by a set of tags.
//
// Can pass literal * to group by all dimensions.
// Example:
//  stream
//      |from()
//          .groupBy(*)
// tick:property
func (s *FromNode) GroupBy(tag ...interface{}) *FromNode {
	s.Dimensions = tag
	return s
}

// If set will include the measurement name in the group ID.
// Along with any other group by dimensions.
//
// Example:
// stream
//      |from()
//          .database('mydb')
//          .groupByMeasurement()
//          .groupBy('host')
//
// The above example selects all measurements from the database 'mydb' and
// then each point is grouped by the host tag and measurement name.
// Thus keeping measurements in their own groups.
// tick:property
func (n *FromNode) GroupByMeasurement() *FromNode {
	n.GroupByMeasurementFlag = true
	return n
}

func (s *FromNode) validate() error {
	return validateDimensions(s.Dimensions, nil)
}
