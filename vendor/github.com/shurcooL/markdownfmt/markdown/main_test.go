package markdown_test

import (
	"bytes"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"testing"

	"github.com/shurcooL/markdownfmt/markdown"
)

func Example() {
	input := []byte(`Title
=

This is a new paragraph. I wonder    if I have too     many spaces.
What about new paragraph.
But the next one...

  Is really new.

1. Item one.
1. Item TWO.


Final paragraph.
`)

	output, err := markdown.Process("", input, nil)
	if err != nil {
		log.Fatalln(err)
	}

	os.Stdout.Write(output)

	// Output:
	// Title
	// =====
	//
	// This is a new paragraph. I wonder if I have too many spaces. What about new paragraph. But the next one...
	//
	// Is really new.
	//
	// 1.	Item one.
	// 2.	Item TWO.
	//
	// Final paragraph.
	//
}

func Example_two() {
	input := []byte(`Title
==

Subtitle
---

How about ` + "`this`" + ` and other stuff like *italic*, **bold** and ***super    extra***.
`)

	output, err := markdown.Process("", input, nil)
	if err != nil {
		log.Fatalln(err)
	}

	os.Stdout.Write(output)

	// Output:
	// Title
	// =====
	//
	// Subtitle
	// --------
	//
	// How about `this` and other stuff like *italic*, **bold** and ***super extra***.
	//
}

const reference = `An h1 header
============

Paragraphs are separated by a blank line.

2nd paragraph. *Italic*, **bold**, ` + "`monospace`" + `. Itemized lists look like:

-	this one
-	that one
-	the other one

Nothing to note here.

> Block quotes are written like so.
>
> > They can be nested.
>
> They can span multiple paragraphs, if you like.

-	Item 1
-	Item 2
	-	Item 2a
		-	Item 2a
	-	Item 2b
-	Item 3

Hmm.

1.	Item 1
2.	Item 2
	1.	Blah.
	2.	Blah.
3.	Item 3
	-	Item 3a
	-	Item 3b

Large spacing...

1.	An entire paragraph is written here, and bigger spacing between list items is desired. This is supported too.

2.	Item 2

	1.	Blah.

	2.	Blah.

3.	Item 3

	-	Item 3a

	-	Item 3b

Last paragraph here.

An h2 header
------------

-	Paragraph right away.
-	**Big item**: Right away after header.

[Visit GitHub!](www.github.com)

![Hmm](http://example.org/image.png)

![Alt text](/path/to/img.jpg "Optional title") ![Alt text](/path/to/img.jpg "Hello \" 世界")

~~Mistaken text.~~

This (**should** be *fine*).

A \> B.

It's possible to backslash escape \<html\> tags and \` + "`" + `backticks\` + "`" + `. They are treated as text.

1986\. What a great season.

The year was 1986. What a great season.

\*literal asterisks\*.

---

http://example.com

Now a [link](www.github.com) in a paragraph. End with [link_underscore.go](www.github.com).

-	[Link](www.example.com)

### An h3 header

Here's a numbered list:

1.	first item
2.	second item
3.	third item

Note again how the actual text starts at 4 columns in (4 characters from the left side). Here's a code sample:

` + "```" + `
# Let me re-iterate ...
for i in 1 .. 10 { do-something(i) }
` + "```" + `

As you probably guessed, indented 4 spaces. By the way, instead of indenting the block, you can use delimited blocks, if you like:

` + "```" + `
define foobar() {
    print "Welcome to flavor country!";
}
` + "```" + `

(which makes copying & pasting easier). You can optionally mark the delimited block for Pandoc to syntax highlight it:

` + "```Go" + `
func main() {
	println("Hi.")
}
` + "```" + `

Here's a table.

| Name  | Age |
|-------|-----|
| Bob   | 27  |
| Alice | 23  |

Colons can be used to align columns.

| Tables        | Are           | Cool      |
|---------------|:-------------:|----------:|
| col 3 is      | right-aligned |     $1600 |
| col 2 is      |   centered!   |       $12 |
| zebra stripes |   are neat    |        $1 |
| support for   | サブタイトル  | priceless |

The outer pipes (|) are optional, and you don't need to make the raw Markdown line up prettily. You can also use inline Markdown.

| Markdown | More      | Pretty     |
|----------|-----------|------------|
| *Still*  | ` + "`renders`" + ` | **nicely** |
| 1        | 2         | 3          |

Nested Lists
============

### Codeblock within list

-	list1

	` + "```" + `C
	if (i == 5)
	    break;
	` + "```" + `

### Blockquote within list

-	list1

	> This a quote within a list.

### Table within list

-	list1

	| Header One | Header Two |
	|------------|------------|
	| Item One   | Item Two   |

### Multi-level nested

-	Item 1

	Another paragraph inside this list item is indented just like the previous paragraph.

-	Item 2

	-	Item 2a

		Things go here.

		> This a quote within a list.

		And they stay here.

	-	Item 2b

-	Item 3

Line Breaks
===========

Some text with two trailing spaces for linebreak.  ` + `
More text immediately after.  ` + `
Useful for writing poems.` + `

Done.
`

func Test(t *testing.T) {
	output, err := markdown.Process("", []byte(reference), nil)
	if err != nil {
		log.Fatalln(err)
	}

	diff, err := diff([]byte(reference), output)
	if err != nil {
		log.Fatalln(err)
	}

	if len(diff) != 0 {
		t.Errorf("Difference of %d lines:\n%s", bytes.Count(diff, []byte("\n")), string(diff))
	}
}

func TestWideChar(t *testing.T) {
	input := []byte(`タイトル
==

サブタイトル
---

aaa/あああ
----------
`)

	expected := []byte(`タイトル
========

サブタイトル
------------

aaa/あああ
----------
`)

	output, err := markdown.Process("", input, nil)
	if err != nil {
		log.Fatalln(err)
	}

	diff, err := diff(expected, output)
	if err != nil {
		log.Fatalln(err)
	}

	if len(diff) != 0 {
		t.Errorf("Difference of %d lines:\n%s", bytes.Count(diff, []byte("\n")), string(diff))
	}
}

func TestLineBreak(t *testing.T) {
	input := []byte("Some text with two trailing spaces for linebreak.  \nMore      spaced      **text**      *immediately*      after      that.         \nMore than two spaces become two.\n")
	expected := []byte("Some text with two trailing spaces for linebreak.  \nMore spaced **text** *immediately* after that.  \nMore than two spaces become two.\n")

	output, err := markdown.Process("", input, nil)
	if err != nil {
		log.Fatalln(err)
	}

	diff, err := diff(expected, output)
	if err != nil {
		log.Fatalln(err)
	}

	if len(diff) != 0 {
		t.Errorf("Difference of %d lines:\n%s", bytes.Count(diff, []byte("\n")), string(diff))
	}
}

// TestDoubleSpacedListEnd tests that when the document ends with a double spaced list,
// an extra blank line isn't appended. See issue #30.
func TestDoubleSpacedListEnd(t *testing.T) {
	const reference = `-	An item.

-	Another time with a blank line in between.
`
	input := []byte(reference)
	expected := []byte(reference)

	output, err := markdown.Process("", input, nil)
	if err != nil {
		log.Fatalln(err)
	}

	diff, err := diff(expected, output)
	if err != nil {
		log.Fatalln(err)
	}

	if len(diff) != 0 {
		t.Errorf("Difference of %d lines:\n%s", bytes.Count(diff, []byte("\n")), string(diff))
	}
}

// https://github.com/shurcooL/markdownfmt/issues/35.
func TestEscapeBackslashesInURLs(t *testing.T) {
	const reference = `[Link](path\\to\\page)

![Image](path\\to\\image)

https://path\\to\\page
`
	input := []byte(reference)
	expected := []byte(reference)

	output, err := markdown.Process("", input, nil)
	if err != nil {
		log.Fatalln(err)
	}

	diff, err := diff(expected, output)
	if err != nil {
		log.Fatalln(err)
	}

	if len(diff) != 0 {
		t.Errorf("Difference of %d lines:\n%s", bytes.Count(diff, []byte("\n")), string(diff))
	}
}

// https://github.com/shurcooL/markdownfmt/issues/20
func TestSuccessiveLines(t *testing.T) {
	input := []byte(`text
text

[link](https://github.com)
text

*italic*
text

**bold**
text

***massive***
text

` + "`" + `noformat` + "`" + `
text

text
[link](https://github.com)

text
*italic*

text
**bold**

text
***massive***

text
` + "`" + `noformat` + "`" + `
`)
	expected := []byte(`text text

[link](https://github.com) text

*italic* text

**bold** text

***massive*** text

` + "`" + `noformat` + "`" + ` text

text [link](https://github.com)

text *italic*

text **bold**

text ***massive***

text ` + "`" + `noformat` + "`" + `
`)

	output, err := markdown.Process("", input, nil)
	if err != nil {
		log.Fatalln(err)
	}

	diff, err := diff(expected, output)
	if err != nil {
		log.Fatalln(err)
	}

	if len(diff) != 0 {
		t.Errorf("Difference of %d lines:\n%s", bytes.Count(diff, []byte("\n")), string(diff))
	}
}

// TODO: Factor out.
func diff(b1, b2 []byte) (data []byte, err error) {
	f1, err := ioutil.TempFile("", "markdownfmt")
	if err != nil {
		return
	}
	defer os.Remove(f1.Name())
	defer f1.Close()

	f2, err := ioutil.TempFile("", "markdownfmt")
	if err != nil {
		return
	}
	defer os.Remove(f2.Name())
	defer f2.Close()

	f1.Write(b1)
	f2.Write(b2)

	data, err = exec.Command("diff", "-u", f1.Name(), f2.Name()).CombinedOutput()
	if len(data) > 0 {
		// diff exits with a non-zero status when the files don't match.
		// Ignore that failure as long as we get output.
		err = nil
	}
	return
}
