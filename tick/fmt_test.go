package tick

import "testing"

func TestFormat(t *testing.T) {
	testCases := []struct {
		script string
		exp    string
	}{
		{
			script: `var x = 1`,
			exp:    "var x = 1\n",
		},
		{
			script: `var x=1`,
			exp:    "var x = 1\n",
		},
		{
			script: `var x=1.0`,
			exp:    "var x = 1.0\n",
		},
		{
			script: `var x=01`,
			exp:    "var x = 01\n",
		},
		{
			script: `var x=0600`,
			exp:    "var x = 0600\n",
		},
		{
			script: `var x=1m`,
			exp:    "var x = 1m\n",
		},
		{
			script: `var x=60s`,
			exp:    "var x = 60s\n",
		},
		{
			script: `var x= /.*/`,
			exp:    "var x = /.*/\n",
		},
		{
			script: `var x= /^\/root\//`,
			exp:    "var x = /^\\/root\\//\n",
		},
		{
			script: `var x=stream()|window().period(10s).every(10s)`,
			exp: `var x = stream()
    |window()
        .period(10s)
        .every(10s)
`,
		},
		{
			script: `var x = stream()
//Window data  
|window()
// Period / Every 10s
.period(10s).every(10s)`,
			exp: `var x = stream()
    // Window data
    |window()
        // Period / Every 10s
        .period(10s)
        .every(10s)
`,
		},
		{
			script: `var x = stream()
@udf()
    .option(
        // Param 1
        1,
        // Param 2
        2,
        // Param 3
        3,
        // Param 4
        4,
                )
`,
			exp: `var x = stream()
    @udf()
        .option(
            // Param 1
            1,
            // Param 2
            2,
            // Param 3
            3,
            // Param 4
            4
        )
`,
		},
		{
			script: `global(lambda: ("a" + (1)) / (( 4 +"b") * ("c")))`,
			exp:    "global(lambda: (\"a\" + 1) / ((4 + \"b\") * \"c\"))\n",
		},
		{
			script: `global(lambda: (1 + 2 - 3 * 4 / 5) < (sin(6)) AND (TRUE OR FALSE))`,
			exp:    "global(lambda: (1 + 2 - 3 * 4 / 5) < sin(6) AND (TRUE OR FALSE))\n",
		},
		{
			script: `global(lambda: 
(1 + 2 - 3 * 4 / 5) 
< 
(sin(6))
AND 
(TRUE
OR (FALSE 
AND TRUE)))`,
			exp: `global(lambda: (1 + 2 - 3 * 4 / 5) <
    sin(6) AND
    (TRUE OR
        (FALSE AND
            TRUE)))
`,
		},
		{
			script: `global(lambda: 
// If this
// is less than that
(1 + 2 - 3 * 4 / 5) 
< (sin(6))
AND 
// more comments.
(TRUE OR FALSE), 'arg',)`,
			exp: `global(
    lambda: 
    // If this
    // is less than that
    (1 + 2 - 3 * 4 / 5) <
    sin(6) AND
    // more comments.
    (TRUE OR FALSE),
    'arg'
)
`,
		},
		{
			script: `// Preserve comments spacing

// Comment block 1
// still 1

// Comment block 2
// still 2

// Preserve per line spacing
//     indented
//fix this line
//


var x = stream
	|from()
		//.measurement('mem')
		.measurement('cpu')

// This should be its own comment block
x |alert()
	

`,
			exp: `// Preserve comments spacing

// Comment block 1
// still 1

// Comment block 2
// still 2

// Preserve per line spacing
//     indented
// fix this line
//
var x = stream
    |from()
        // .measurement('mem')
        .measurement('cpu')

// This should be its own comment block
x
    |alert()
`,
		},
		{
			script: `// Comment all the things
var 
x = 
stream()
// 1
|
udf()
// 2
    .option(
        // 3
        1,
        // 4
        2.0,
        // 5
        3h,
        // 6
        'a',
    )
// 7
|
eval(
// 8
lambda:
a * b + c
,
)
// 9
|
groupBy(
//10 
*
)
// 11
`,
			exp: `// Comment all the things
var x = stream()
    // 1
    |udf()
        // 2
        .option(
            // 3
            1,
            // 4
            2.0,
            // 5
            3h,
            // 6
            'a'
        )
    // 7
    |eval(
        // 8
        lambda: a * b + c
    )
    // 9
    |groupBy(
        // 10
        *
    )

// 11

`,
		},
		{
			script: `
			// Define a result that contains the most recent score per player.
var topPlayerScores = stream
    |from().measurement('scores')
    // Get the most recent score for each player per game.
// Not likely that a player is playing two games but just in case.
.groupBy('game', 'player')
    |window()
        // keep a buffer of the last 11s of scores
        // just in case a player score hasn't updated in a while
        .period(11s)
        // Emit the current score per player every second.
.every(1s)
        // Align the window boundaries to be on the second.
.align()
    |last('value')

// Calculate the top 15 scores per game
var topScores = topPlayerScores
    |groupBy('game')
    |top(15, 'last', 'player')

// Expose top scores over the HTTP API at the 'top_scores' endpoint.
// Now your app can just request the top scores from Kapacitor
// and always get the most recent result.
//
// http://localhost:9092/api/v1/top_scores/top_scores
topScores
   |httpOut('top_scores')

// Sample the top scores and keep a score once every 10s
var topScoresSampled = topScores
    |sample(10s)

// Store top fifteen player scores in InfluxDB.
topScoresSampled
    |influxDBOut()
        .database('game')
        .measurement('top_scores')

// Calculate the max and min of the top scores.
var max = topScoresSampled
    |max('top')
var min = topScoresSampled
    |min('top')

// Join the max and min streams back together and calculate the gap.
max|join(min)
        .as('max', 'min')
    // calculate the difference between the max and min scores.
|eval(lambda: "max.max" - "min.min", lambda: "max.max", lambda: "min.min")
        .as('gap', 'topFirst', 'topLast')
    // store the fields: gap, topFirst, and topLast in InfluxDB.
|influxDBOut()
        .database('game')
        .measurement('top_scores_gap')
`,
			exp: `// Define a result that contains the most recent score per player.
var topPlayerScores = stream
    |from()
        .measurement('scores')
        // Get the most recent score for each player per game.
        // Not likely that a player is playing two games but just in case.
        .groupBy('game', 'player')
    |window()
        // keep a buffer of the last 11s of scores
        // just in case a player score hasn't updated in a while
        .period(11s)
        // Emit the current score per player every second.
        .every(1s)
        // Align the window boundaries to be on the second.
        .align()
    |last('value')

// Calculate the top 15 scores per game
var topScores = topPlayerScores
    |groupBy('game')
    |top(15, 'last', 'player')

// Expose top scores over the HTTP API at the 'top_scores' endpoint.
// Now your app can just request the top scores from Kapacitor
// and always get the most recent result.
//
// http://localhost:9092/api/v1/top_scores/top_scores
topScores
    |httpOut('top_scores')

// Sample the top scores and keep a score once every 10s
var topScoresSampled = topScores
    |sample(10s)

// Store top fifteen player scores in InfluxDB.
topScoresSampled
    |influxDBOut()
        .database('game')
        .measurement('top_scores')

// Calculate the max and min of the top scores.
var max = topScoresSampled
    |max('top')

var min = topScoresSampled
    |min('top')

// Join the max and min streams back together and calculate the gap.
max
    |join(min)
        .as('max', 'min')
    // calculate the difference between the max and min scores.
    |eval(lambda: "max.max" - "min.min", lambda: "max.max", lambda: "min.min")
        .as('gap', 'topFirst', 'topLast')
    // store the fields: gap, topFirst, and topLast in InfluxDB.
    |influxDBOut()
        .database('game')
        .measurement('top_scores_gap')
`,
		},
	}

	for _, tc := range testCases {
		got, err := Format(tc.script)
		if err != nil {
			t.Fatal(err)
		}
		if got != tc.exp {
			t.Fatalf("unexpected format:\nexp:\n%s\ngot:\n%s\nlength exp:%d got:%d", tc.exp, got, len(tc.exp), len(got))
		}
	}
}
