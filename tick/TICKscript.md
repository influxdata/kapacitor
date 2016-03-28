---
title: TICKscript Specification
---

Introduction
------------

The TICKscript language is an invocation chaining language used to define data processing pipelines.


Notation
-------

The syntax is specified using Extended Backus-Naur Form (“EBNF”).
EBNF is the same notation used in the [Go](http://golang.org/) programming language specification, which can be found [here](https://golang.org/ref/spec).

```
Production  = production_name "=" [ Expression ] "." .
Expression  = Alternative { "|" Alternative } .
Alternative = Term { Term } .
Term        = production_name | token [ "…" token ] | Group | Option | Repetition .
Group       = "(" Expression ")" .
Option      = "[" Expression "]" .
Repetition  = "{" Expression "}" .
```

Notation operators in order of increasing precedence:

```
|   alternation
()  grouping
[]  option (0 or 1 times)
{}  repetition (0 to n times)
```

Grammar
-------

The following is the EBNF grammar definition of TICKscript.

```

unicode_char        = (* an arbitrary Unicode code point except newline *) .
digit               = "0" … "9" .
ascii_letter        = "A" … "Z" | "a" … "z" .
letter              = ascii_letter | "_" .
identifier          = ( letter ) { letter | digit } .
boolean_lit         = "TRUE" | "FALSE" .
int_lit             = "1" … "9" { digit }
letter              = ascii_letter | "_" .
number_lit          = digit { digit } { "." {digit} } .
duration_lit        = int_lit duration_unit .
duration_unit       = "u" | "µ" | "ms" | "s" | "m" | "h" | "d" | "w" .
string_lit          = `'` { unicode_char } `'` .
star_lit            = "*"
regex_lit           = `/` { unicode_char } `/` .

operator_lit       = "+" | "-" | "*" | "/" | "==" | "!=" |
                     "<" | "<=" | ">" | ">=" | "=~" | "!~" |
                     "AND" | "OR" .

Program      = Statement { Statement } .
Statement    = Declaration | Expression .
Declaration  = "var" identifier "=" Expression .
Expression   = identifier { Chain } | Function { Chain } | Primary .
Chain        = "@" Function | "|" Function { Chain } | "." Function { Chain} | "." identifier { Chain } .
Function     = identifier "(" Parameters ")" .
Parameters   = { Parameter "," } [ Parameter ] .
Parameter    = Expression | "lambda:" LambdaExpr | Primary .
Primary      = "(" LambdaExpr ")" | number_lit | string_lit |
                boolean_lit | duration_lit | regex_lit | star_lit |
                LFunc | identifier | Reference | "-" Primary | "!" Primary .
Reference    = `"` { unicode_char } `"` .
LambdaExpr   =  Primary operator_lit Primary .
LFunc        = identifier "(" LParameters ")"
LParameters  = { LParameter "," } [ LParameter ] .
LParameter   = LambdaExpr |  Primary .

```


