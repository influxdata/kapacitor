package tick

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParserLookAhead(t *testing.T) {
	assert := assert.New(t)

	tree := &tree{}
	tree.lex = lex("0 1 2 3")

	assert.Equal(token{tokenNumber, 0, "0"}, tree.next())
	assert.Equal(token{tokenNumber, 2, "1"}, tree.peek())
	assert.Equal(token{tokenNumber, 2, "1"}, tree.next())
	tree.backup()
	assert.Equal(token{tokenNumber, 2, "1"}, tree.next())
	assert.Equal(token{tokenNumber, 4, "2"}, tree.peek())
	tree.backup()
	assert.Equal(token{tokenNumber, 2, "1"}, tree.next())
	assert.Equal(token{tokenNumber, 4, "2"}, tree.next())
	assert.Equal(token{tokenNumber, 6, "3"}, tree.next())
}

func TestParseErrors(t *testing.T) {
	assert := assert.New(t)

	type testCase struct {
		Text  string
		Error string
	}

	test := func(tc testCase) {
		_, err := parse(tc.Text)
		if assert.NotNil(err) {
			assert.Equal(tc.Error, err.Error())
		}
	}

	cases := []testCase{
		testCase{
			Text:  "a",
			Error: `parser: unexpected EOF line 1 char 2 in "a". expected: ";"`,
		},
		testCase{
			Text:  "a;\n\n\nvar b = stream",
			Error: `parser: unexpected EOF line 4 char 15 in "b = stream". expected: ";"`,
		},
		testCase{
			Text:  "a;\n\n\nvar b = stream.window()period",
			Error: `parser: unexpected "period" line 4 char 24 in "m.window()period". expected: ";"`,
		},
		testCase{
			Text:  "a;\n\n\nvar b = stream.window\nb.period(10s);",
			Error: `parser: unexpected "b" line 5 char 1 in "am.window\nb.period(1". expected: "("`,
		},
	}

	for _, tc := range cases {
		test(tc)
	}
}

func TestParseSingleStmt(t *testing.T) {
	assert := assert.New(t)

	script := `
var x = stream
		.window()
		.period(5m)
		.every(1m)
		.map(influxql.agg.mean, "value");
`
	tree, err := parse(script)
	assert.Nil(err)

	// Expect a tree like so:
	/*
	           Root
	            |
	          (b0 =)
	           / \
	          /   \
	         /     \
	       (x)     (b1 .)
	                / \
	               /   \
	              /     \
	         (map)       (b2 .)
	          / \          |   \
	         /   \         |    \
	        /     \        |     \
	    (b5 .)  ("value") (every) (b3 .)
	     /  \               |       / \
	    /    \             (1m)    /   \
	   /      \                   /     \
	 (mean) (b6 .)            (period) (b4 .)
	         /  \                |       / \
	        /    \              (5m)    /   \
	       /      \                    /     \
	    (agg)  (influxql)         (window) (stream)
	*/
	//Notice the inverted nature of the tree. This structure makes evaluating the tree recursive depth first.

	//Assert we have a list of one statement
	l, ok := tree.Root.(*listNode)
	if !assert.True(ok, "tree.Root is not a list node") {
		t.FailNow()
	}
	if !assert.Equal(1, len(l.Nodes), "Did not get exactly one node in statement list") {
		t.FailNow()
	}

	//Assert the first statement is a binary node with operator '='
	b0, ok := l.Nodes[0].(*binaryNode)
	if !assert.True(ok, "first statement is not a binary node %q", l.Nodes[0]) {
		t.FailNow()
	}
	assert.Equal("=", b0.Operator.val)

	//Assert b0.left is an ident node of 'x'
	varX, ok := b0.Left.(*identNode)
	if !assert.True(ok, "b0.left is not an ident node %q", b0.Left) {
		t.FailNow()
	}
	assert.Equal("x", varX.Ident)

	//Assert b0.right is a binary node of operator '.'
	b1, ok := b0.Right.(*binaryNode)
	if !assert.True(ok, "b0.right is not a binary node %q", b0.Right) {
		t.FailNow()
	}
	assert.Equal(".", b1.Operator.val)

	//Assert b1.left is func node 'map'
	fMap, ok := b1.Left.(*funcNode)
	if !assert.True(ok, "b1.left is not func node %q", b1.Left) {
		t.FailNow()
	}
	assert.Equal("map", fMap.Func)
	if assert.Equal(2, len(fMap.Args)) {

		//Assert first arg to 'map' is binary node
		b5, ok := fMap.Args[0].(*binaryNode)
		if !assert.True(ok, "First argument to 'map' is not binary node %q", fMap.Args[0]) {
			t.FailNow()
		}

		//Assert b5.left is ident node 'mean'
		mean, ok := b5.Left.(*identNode)
		if assert.True(ok, "b5.left is not a ident node %q", b5.Left) {
			assert.Equal("mean", mean.Ident)
		}

		//Assert b5.right is binary node of operator '.'
		b6, ok := b5.Right.(*binaryNode)
		if assert.True(ok, "b5.right is not a binary node %q", b5.Right) {
			assert.Equal(".", b6.Operator.val)
		}

		//Assert b6.left is ident node 'agg'
		agg, ok := b6.Left.(*identNode)
		if assert.True(ok, "b6.left is not a ident node %q", b6.Left) {
			assert.Equal("agg", agg.Ident)
		}

		//Assert b6.right is ident node 'influxql'
		influxql, ok := b6.Right.(*identNode)
		if assert.True(ok, "b6.right is not a ident node %q", b6.Right) {
			assert.Equal("influxql", influxql.Ident)
		}

		//Assert second arg is string node 'value'
		value, ok := fMap.Args[1].(*stringNode)
		if assert.True(ok, "Second argument to 'map' is not string node %q", fMap.Args[0]) {
			assert.Equal("value", value.Literal)
		}
	}

	//Assert b1.right is binary node of operator '.'
	b2, ok := b1.Right.(*binaryNode)
	if !assert.True(ok, "b1.right is not a binary node %q", b1.Right) {
		t.FailNow()
	}
	assert.Equal(".", b2.Operator.val)

	//Assert b2.left is func node 'every'
	fEvery, ok := b2.Left.(*funcNode)
	if !assert.True(ok, "b2.left is not func node %q", b2.Left) {
		t.FailNow()
	}
	assert.Equal("every", fEvery.Func)
	if assert.Equal(1, len(fEvery.Args)) {
		d, ok := fEvery.Args[0].(*durationNode)
		if assert.True(ok, "First argument to 'every' is not duration node %q", fEvery.Args[0]) {
			assert.Equal(time.Minute, d.Dur)
		}
	}

	//Assert b2.right is binary node of operator '.'
	b3, ok := b2.Right.(*binaryNode)
	if !assert.True(ok, "b2.right is not a binary node %q", b2.Right) {
		t.FailNow()
	}
	assert.Equal(".", b3.Operator.val)

	//Assert b3.left is func node 'period'
	fPeriod, ok := b3.Left.(*funcNode)
	if !assert.True(ok, "b3.left is not func node %q", b3.Left) {
		t.FailNow()
	}
	assert.Equal("period", fPeriod.Func)
	if assert.Equal(1, len(fPeriod.Args)) {
		d, ok := fPeriod.Args[0].(*durationNode)
		if assert.True(ok, "First argument to 'period' is not duration node %q", fPeriod.Args[0]) {
			assert.Equal(time.Minute*5, d.Dur)
		}
	}

	//Assert b3.right is a binary node of operator '.'
	b4, ok := b3.Right.(*binaryNode)
	if !assert.True(ok, "b3.right is not a binary node %q", b3.Right) {
		t.FailNow()
	}
	assert.Equal(".", b4.Operator.val)

	//Assert b4.left is func node 'window'
	fWindow, ok := b4.Left.(*funcNode)
	if !assert.True(ok, "b4.left is not func node %q", b4.Left) {
		t.FailNow()
	}
	assert.Equal("window", fWindow.Func)
	assert.Equal(0, len(fWindow.Args))

	//Assert b4.right is ident node 'stream'
	iStream, ok := b4.Right.(*identNode)
	if !assert.True(ok, "b4.right is not ident node %q", b4.Right) {
		t.FailNow()
	}
	assert.Equal("stream", iStream.Ident)

}
