package pipeline

// The WhereNode filters the data stream by a given expression.
// See `Expressions` for documentation on the syntax and behavior
// of expressions.
//
// Example:
//    // Define a basic batch node that queries for idle cpu.
//    var cpu = batch
//        .query('''
//               SELECT mean("idle")
//               FROM "tests"."default".cpu
//               WHERE dc = 'nyc'
//        ''')
//        .period(10s)
//        .groupBy(time(2s))
//    // Filter down a fork of the cpu data for serverA
//    cpu
//        .fork()
//        .where("host = 'serverA'")
//        .mapReduce(influxql.top("mean", 10)
//        .window()
//            .period(1m)
//            .every(1m)
//        .httpOut("serverA")
//    // Filter down a fork of the cpu data for serverB
//    cpu
//        .fork()
//        .where("host = 'serverB'")
//        .mapReduce(influxql.top("mean", 10)
//        .window()
//            .period(1m)
//            .every(1m)
//        .httpOut("serverB")
//
type WhereNode struct {
	chainnode
	// The expression predicate.
	// tick:ignore
	Predicate string
}

func newWhereNode(wants EdgeType, predicate string) *WhereNode {
	return &WhereNode{
		chainnode: newBasicChainNode("where", wants, wants),
		Predicate: predicate,
	}
}
