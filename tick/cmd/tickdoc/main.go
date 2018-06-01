// Tickdoc is a simple utility similar to godoc that generates documentation from comments.
//
// The 'tickdoc' utility understands several special comments to help it generate clean documentation.
//
// 1. tick:ignore -- can be added to any field, method, function or struct and tickdoc will simply skip it
// and not generate any documentation for it. Useful for ignore fields that are set via property methods.
//
// 2. tick:property -- is only added to methods and informs tickdoc that the method is a property method not a chaining method.
//
// 3. tick:embedded:[NODE_NAME].[PROPERTY_NAME] -- The object's properties are embedded into a parent node's property identified by NODE_NAME.PROPERTY_NAME.
//
// 4. tick:wraps:[INTERNAL_FIELD] - use the comments associate with the internal field to describe the wrapper type
//
// Just place one of these comments on a line all by itself and tickdoc will find it and behave accordingly.
//
// Example:
//    // Normal comments
//    //
//    // Other comments
//    // tick:ignore
//    type A struct{}
//
// Tickdoc will format examples like godoc but assumes the examples are TICKscript instead of
// golang code and styles them accordingly.
//
// Otherwise just document your code normally and tickdoc will do the rest.
package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	_ "html"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"text/template"
	"unicode"

	"github.com/BurntSushi/toml"
	"github.com/serenize/snaker"
	"github.com/shurcooL/markdownfmt/markdown"
)

// The weight difference between two pages.

const tickIgnore = "tick:ignore"
const tickProperty = "tick:property"
const tickWraps = "tick:wraps"
const tickExample = "Example:"
const tickLang = "javascript"

var tickEmbedded = regexp.MustCompile(`^tick:embedded:(\w+Node).(\w+)$`)

var configPath = flag.String("config", "tickdoc.conf", "path to tickdoc configuration file.")

var config Config

var usageStr = `Usage: %s [options] [package dir] [output dir]

Options:
`

func usage() {
	fmt.Fprintf(os.Stderr, usageStr, os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Usage = usage
	flag.Parse()
	args := flag.Args()

	if len(args) != 2 {
		flag.Usage()
		os.Exit(1)
	}

	dir := args[0]
	out := args[1]
	// Decode config
	err := decodeConfig(*configPath)
	if err != nil {
		log.Fatal(err)
	}

	fset := token.NewFileSet() // positions are relative to fset

	skipTest := func(fi os.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}

	pkgs, err := parser.ParseDir(fset, dir, skipTest, parser.ParseComments)
	if err != nil {
		log.Fatal(err)
	}

	nodes := make(map[string]*Node)
	for _, pkg := range pkgs {
		f := ast.MergePackageFiles(pkg, ast.FilterFuncDuplicates|ast.FilterUnassociatedComments|ast.FilterImportDuplicates)
		ast.Inspect(f, func(n ast.Node) bool {
			switch decl := n.(type) {
			case *ast.GenDecl:
				handleGenDecl(nodes, decl)
			case *ast.FuncDecl:
				handleFuncDecl(nodes, decl)
			}
			return true
		})
	}

	ordered := make([]string, 0, len(nodes))
	for name, node := range nodes {
		if name == "" || !ast.IsExported(name) || node.Name == "" || isAnonField(node, nodes) {
			continue
		}
		if node.Embedded {
			err := node.Embed(nodes)
			if err != nil {
				log.Fatal(err)
			}
		} else {
			ordered = append(ordered, name)
			node.Flatten(nodes)
		}
	}
	sort.Strings(ordered)

	r := markdown.NewRenderer(nil)
	for i, name := range ordered {
		var buf bytes.Buffer
		n := nodes[name]
		weight := (i + 1) * config.IndexWidth
		if w, ok := config.Weights[name]; ok {
			weight = w
		}
		n.Render(&buf, r, nodes, weight)
		filename := filepath.Join(out, snaker.CamelToSnake(name)+".md")
		log.Println("Writing file:", filename, i)
		f, err := os.Create(filename)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		f.Write(buf.Bytes())
	}
}

func decodeConfig(path string) (err error) {
	if _, err = toml.DecodeFile(path, &config); err != nil {
		return err
	}
	config.headerTemplate, err = template.New("header").Parse(config.PageHeader)
	if err != nil {
		return fmt.Errorf("invalid page header template: %s", err)
	}
	return nil
}

func handleGenDecl(nodes map[string]*Node, decl *ast.GenDecl) {
	if shouldIgnore(decl.Doc) {
		return
	}
	if decl.Tok == token.TYPE {
		t := decl.Specs[0].(*ast.TypeSpec)
		if s, ok := t.Type.(*ast.StructType); ok {
			node := nodes[t.Name.Name]
			if node == nil {
				node = newNode()
				nodes[t.Name.Name] = node
			}
			node.Name = t.Name.Name
			node.Doc = decl.Doc
			node.Embedded, node.EmbeddedParent, node.EmbeddedProperty = isEmbedded(decl.Doc)
			processFields(node, s)
			getWrapped(node)
		}
	}
}

func processFields(node *Node, s *ast.StructType) {
	for _, field := range s.Fields.List {
		if shouldIgnore(field.Doc) {
			continue
		}
		if field.Names == nil {
			//Anonymous field
			if i, ok := field.Type.(*ast.Ident); ok {
				parent := i.Name
				node.AnonFields = append(node.AnonFields, parent)
			} else if s, ok := field.Type.(*ast.StarExpr); ok {
				if i, ok := s.X.(*ast.Ident); ok {
					parent := i.Name
					node.AnonFields = append(node.AnonFields, parent)
				}
			}
		} else if len(field.Names) == 1 {
			name := field.Names[0].Name
			if !ast.IsExported(name) {
				continue
			}
			ptype := resolveTypeExpr(field.Type)
			node.Properties[name] = &Property{
				Name:   name,
				Doc:    field.Doc,
				Params: []Param{{"value", ptype}},
			}
		}
	}

}

func handleFuncDecl(nodes map[string]*Node, decl *ast.FuncDecl) {
	if shouldIgnore(decl.Doc) {
		return
	}
	name := decl.Name.Name
	if ast.IsExported(name) && decl.Recv != nil {
		// Find receive node
		self := ""
		if t, ok := decl.Recv.List[0].Type.(*ast.StarExpr); ok {
			if i, ok := t.X.(*ast.Ident); ok {
				self = i.Name
			}
		}
		node := nodes[self]
		if node == nil {
			node = newNode()
			nodes[self] = node
		}
		typ := determineFuncType(decl.Doc)
		// Get params
		params := make([]Param, 0)
		for _, param := range decl.Type.Params.List {
			ptype := resolveTypeExpr(param.Type)
			for _, name := range param.Names {
				params = append(params, Param{name.Name, ptype})
			}
		}
		if typ == ChainFunc {

			// Get result
			log.Println(decl)
			result := decl.Type.Results.List[0]
			rtype := resolveTypeExpr(result.Type)

			// Define method
			node.Methods[name] = &Method{
				Name:   name,
				Doc:    decl.Doc,
				Params: params,
				Result: rtype,
			}
		} else if typ == PropertyFunc {
			// Define Property
			node.Properties[name] = &Property{
				Name:   name,
				Doc:    decl.Doc,
				Params: params,
			}
		}
	}
}

func shouldIgnore(cg *ast.CommentGroup) bool {
	if cg == nil {
		return false
	}
	for _, l := range cg.List {
		s := strings.TrimSpace(strings.TrimLeft(l.Text, "/"))
		if s == tickIgnore {
			return true
		}
	}
	return false
}

func isAnonField(nd *Node, nodes map[string]*Node) bool {
	for _, node := range nodes {
		for _, field := range node.AnonFields {
			if field == nd.Name {
				return true
			}
		}
	}
	return false
}

// some nodes are wrappers - e.g. AlertNode{ *AlertNodeData }
// this looks into the wrapped field
func getWrapped(nd *Node) string {
	if nd.Doc == nil {
		return ""
	}
	for _, ln := range nd.Doc.List {
		s := strings.TrimSpace(strings.TrimLeft(ln.Text, "/"))
		if strings.Contains(s, tickWraps) {
			tokens := strings.Split(s, ":")
			return tokens[2]
		}
	}

	return ""
}

func getConstructor(target string, sources ...*Node) *Method {

	for _, source := range sources {
		for _, meth := range source.Methods {
			if strings.ToUpper(meth.Name) == strings.ToUpper(target) {
				return meth
			}
		}
	}
	return nil
}

func isEmbedded(cg *ast.CommentGroup) (bool, string, string) {
	if cg == nil {
		return false, "", ""
	}
	for _, l := range cg.List {
		s := strings.TrimSpace(strings.TrimLeft(l.Text, "/"))
		if matches := tickEmbedded.FindStringSubmatch(s); len(matches) == 3 {
			return true, matches[1], matches[2]
		}
	}
	return false, "", ""
}

type FuncType int

const (
	PropertyFunc FuncType = iota
	ChainFunc
)

func determineFuncType(cg *ast.CommentGroup) FuncType {
	if cg == nil {
		return ChainFunc
	}
	for i, l := range cg.List {
		s := strings.TrimSpace(strings.TrimLeft(l.Text, "/"))
		if s == tickProperty {
			cg.List = append(cg.List[:i], cg.List[i+1:]...)
			return PropertyFunc
		}
	}
	return ChainFunc
}

func resolveTypeExpr(n ast.Expr) string {
	switch t := n.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.StarExpr:
		return resolveTypeExpr(t.X)
	case *ast.Ellipsis:
		return "..." + resolveTypeExpr(t.Elt)
	case *ast.SelectorExpr:
		return resolveTypeExpr(t.X) + "." + t.Sel.Name
	case *ast.MapType:
		return fmt.Sprintf("map[%s]%s", resolveTypeExpr(t.Key), resolveTypeExpr(t.Value))
	case *ast.InterfaceType:
		return "interface{}"
	case *ast.ArrayType:
		return "[]" + resolveTypeExpr(t.Elt)
	default:
		log.Printf("unsupported expr type: %T\n", n)
	}
	return ""
}

func nameToTickName(name string) string {
	return strings.ToLower(name[0:1]) + name[1:]
}

func nodeNameToTickName(name string) string {
	result := strings.TrimSuffix(name, "Node")
	if strings.HasPrefix(strings.ToUpper(result), "HTTP") {
		return strings.ToLower(result[0:4]) + result[4:]
	}

	return nameToTickName(result)
}

func nodeNameToLink(name string) string {
	return fmt.Sprintf("%s/%s/", config.Root, snaker.CamelToSnake(name))
}

func methodNameToLink(node, name string) string {
	return fmt.Sprintf("%s#%s", nodeNameToLink(node), strings.ToLower(name))
}

func renderDoc(buf *bytes.Buffer, nodes map[string]*Node, r Renderer, doc *ast.CommentGroup) {
	if doc == nil {
		return
	}

	var lines bytes.Buffer
	for i := 0; i < len(doc.List); i++ {
		s := strings.TrimSpace(strings.TrimLeft(doc.List[i].Text, "/"))
		lines.Write(addNodeLinks(nodes, s))
		lines.Write([]byte("\n"))
		if s == tickExample {
			r.Paragraph(buf, func() bool {
				buf.Write(lines.Bytes())
				lines.Reset()
				return true
			})
			var code bytes.Buffer
			for i++; i < len(doc.List); i++ {
				s := strings.TrimLeft(doc.List[i].Text, "/")
				if s == "" {
					break
				}
				code.Write([]byte(s))
				code.Write([]byte("\n"))
			}
			r.BlockCode(buf, code.Bytes(), tickLang)
		}
	}
	r.Paragraph(buf, func() bool {
		buf.Write(lines.Bytes())
		lines.Reset()
		return true
	})

}

func addNodeLinks(nodes map[string]*Node, line string) []byte {
	var buf bytes.Buffer
	scan := bufio.NewScanner(strings.NewReader(line))
	scan.Split(bufio.ScanWords)
	for scan.Scan() {
		word := strings.TrimFunc(scan.Text(), unicode.IsPunct)
		parts := strings.Split(word, ".")
		node := word
		method := ""
		if len(parts) == 2 {
			node = parts[0]
			method = parts[1]
		}
		if nodes[node] != nil && ast.IsExported(node) {
			buf.Write([]byte("["))
			buf.Write(scan.Bytes())
			buf.Write([]byte("]("))
			if method == "" {
				buf.Write([]byte(nodeNameToLink(node)))
			} else {
				buf.Write([]byte(methodNameToLink(node, method)))
			}
			buf.Write([]byte(") "))
		} else {
			buf.Write(scan.Bytes())
			buf.Write([]byte(" "))
		}
	}
	return buf.Bytes()
}

type Node struct {
	Name             string
	Doc              *ast.CommentGroup
	Properties       map[string]*Property
	Methods          map[string]*Method
	AnonFields       []string
	Embedded         bool
	EmbeddedParent   string
	EmbeddedProperty string
}

func newNode() *Node {
	return &Node{
		Properties: make(map[string]*Property),
		Methods:    make(map[string]*Method),
	}
}

// Recurse up through anonymous fields and flatten list of methods etc.
func (n *Node) Flatten(nodes map[string]*Node) {
	for _, anon := range n.AnonFields {
		node := nodes[anon]
		if node != nil {
			node.Flatten(nodes)
			for name, m := range node.Properties {
				_, hasP := n.Properties[name]
				_, hasM := n.Methods[name]
				if !hasP && !hasM {
					n.Properties[name] = m
				}
			}
			for name, m := range node.Methods {
				_, hasP := n.Properties[name]
				_, hasM := n.Methods[name]
				if !hasP && !hasM {
					n.Methods[name] = m
				}
			}
		}
	}
}

func (n *Node) Embed(nodes map[string]*Node) error {
	parent := nodes[n.EmbeddedParent]
	if parent == nil {
		return fmt.Errorf("no node %s", n.EmbeddedParent)
	}
	if prop, ok := parent.Properties[n.EmbeddedProperty]; ok {
		prop.EmbeddedProperties = n.Properties
	} else if len(parent.AnonFields) > 0 {
		anon_parent := nodes[parent.AnonFields[0]]
		if anon_parent == nil {
			return fmt.Errorf("no node %s from anon_field in node %s", parent.AnonFields[0], n.EmbeddedParent)
		}

		if prop, ok := anon_parent.Properties[n.EmbeddedProperty]; ok {
			prop.EmbeddedProperties = n.Properties
		} else {
			return fmt.Errorf("no property %s no node %s not even in node from AnonField %s", n.EmbeddedProperty, n.EmbeddedParent, parent.AnonFields[0])
		}
	} else {
		return fmt.Errorf("no property %s no node %s", n.EmbeddedProperty, n.EmbeddedParent)
	}
	return nil
}

type headerInfo struct {
	Title      string
	Name       string
	Identifier string
	Weight     int
}

func (n *Node) renderConstructor(buf *bytes.Buffer, nodes map[string]*Node) {

	shortName := strings.TrimSuffix(n.Name, "Node")

	buf.Write([]byte("### Constructor \n\n| Chaining Method | Description |\n|:---------|:---------|\n"))
	constructor := getConstructor(shortName, n, nodes["chainnode"], nodes["BatchNode"], nodes["StreamNode"])
	if constructor == nil {
		buf.Write([]byte("| **[" + nodeNameToTickName(n.Name) + "](#descr)** | Has no constructor signature. |\n"))
	} else {

		buf.Write([]byte("| "))
		buf.Write([]byte(fmt.Sprintf("**[%s](#descr)&nbsp;(&nbsp;", nameToTickName(constructor.Name))))

		for i, param := range constructor.Params {
			buf.Write([]byte(fmt.Sprintf("`%s`&nbsp;`%s`", param.Name, param.Type)))
			if (i + 1) < len(constructor.Params) {
				buf.Write([]byte(",&nbsp;"))
			}
		}

		buf.Write([]byte(")**"))

		buf.Write([]byte(" | "))

		for _, line := range constructor.Doc.List {
			//Use only first paragraph - so stop on empty line
			if len(strings.TrimSpace(strings.TrimLeft(fmt.Sprint(line.Text), "/"))) == 0 {
				break
			}
			buf.Write([]byte(strings.TrimSpace(strings.TrimLeft(fmt.Sprint(line.Text), "/"))))
			buf.Write([]byte(" "))
		}
		buf.Write([]byte(" |\n\n"))
	}

}

func (n *Node) renderPropertiesTable(buf *bytes.Buffer, nodes map[string]*Node) {
	buf.Write([]byte("### Property Methods\n"))

	if len(n.Properties) > 0 {

		props := make([]string, len(n.Properties))
		i := 0

		for name, _ := range n.Properties {
			props[i] = name
			i++
		}
		sort.Strings(props)

		buf.Write([]byte("\n"))
		buf.Write([]byte("| Setters | Description |\n|:---|:---|\n"))

		for _, name := range props {
			n.Properties[name].RenderAsRow(buf, nodes)
		}

		buf.Write([]byte("\n\n"))
	} else {
		buf.Write([]byte("This node has no properties that can be set.\n\n"))
	}

}

func (n *Node) Render(buf *bytes.Buffer, r Renderer, nodes map[string]*Node, weight int) error {

	properties := make([]string, len(n.Properties))
	i := 0
	for name := range n.Properties {
		properties[i] = name
		i++
	}
	sort.Strings(properties)

	methods := make([]string, len(n.Methods))
	i = 0
	for name := range n.Methods {
		methods[i] = name
		i++
	}
	sort.Strings(methods)

	info := headerInfo{
		Title:      n.Name,
		Name:       strings.Replace(n.Name, "Node", "", 1),
		Identifier: snaker.CamelToSnake(n.Name),
		Weight:     weight,
	}
	config.headerTemplate.Execute(buf, info)

	n.renderConstructor(buf, nodes)

	n.renderPropertiesTable(buf, nodes)

	r.Header(buf, func() bool { buf.Write([]byte("Chaining Methods")); return true }, 3, "")

	for i, name := range methods {
		buf.Write([]byte(fmt.Sprintf("[%s](%s)", name, methodNameToLink(n.Name, name))))
		if (i + 1) < len(methods) {
			buf.Write([]byte(", "))
		}
	}

	// Need to add a few lines to push description below search bar
	// when jumping to it from internal link in Constructor table
	buf.Write([]byte("\n<a id='descr'/><hr/><br/>\n### Description\n"))

	// some nodes are wrappers - e.g. AlertNode{ *AlertNodeData }
	// if so - use Doc for wrapped field instead
	if len(getWrapped(n)) > 0 {
		renderDoc(buf, nodes, r, nodes[getWrapped(n)].Doc)
	} else {
		renderDoc(buf, nodes, r, n.Doc)
	}

	buf.Write([]byte("\n<a href=\"javascript:document.getElementsByClassName('article')[0].scrollIntoView();\" title=\"top\">^</a>\n"))

	// Properties
	if len(n.Properties) > 0 {
		r.Header(buf, func() bool { buf.Write([]byte("Properties")); return true }, 2, "")
		r.Paragraph(buf, func() bool {
			buf.Write([]byte(config.PropertyMethodDesc))
			return true
		})
		renderProperties(buf, r, n.Properties, nodes, 3, nodeNameToTickName(n.Name), "")
	}

	// Methods
	if len(methods) > 0 {
		r.Header(buf, func() bool { buf.Write([]byte("Chaining Methods")); return true }, 2, "")
		r.Paragraph(buf, func() bool {
			buf.Write([]byte(config.ChainMethodDesc))
			return true
		})
		for _, name := range methods {
			n.Methods[name].Render(buf, r, nodes, nodeNameToTickName(n.Name))
			buf.Write([]byte("\n"))
			buf.Write([]byte("<a href=\"javascript:document.getElementsByClassName('article')[0].scrollIntoView();\" title=\"top\">^</a>\n"))
		}
	}

	return nil
}

func renderProperties(buf *bytes.Buffer, r Renderer, properties map[string]*Property, nodes map[string]*Node, header int, node, namePrefix string) {
	props := make([]string, len(properties))
	i := 0
	for name, _ := range properties {
		props[i] = name
		i++
	}
	sort.Strings(props)
	for _, name := range props {
		properties[name].Render(buf, r, nodes, header, node, namePrefix)
		buf.Write([]byte("\n"))
	}
}

type Property struct {
	Name               string
	Doc                *ast.CommentGroup
	Params             []Param
	EmbeddedProperties map[string]*Property
}

func (p *Property) Render(buf *bytes.Buffer, r Renderer, nodes map[string]*Node, header int, node, namePrefix string) error {
	r.Header(buf, func() bool { buf.Write([]byte(namePrefix + p.Name)); return true }, header, "")

	renderDoc(buf, nodes, r, p.Doc)

	var code bytes.Buffer
	code.Write([]byte(node))
	code.Write([]byte("."))
	code.Write([]byte(nameToTickName(p.Name)))
	code.Write([]byte("("))
	for i, param := range p.Params {
		if i != 0 {
			code.Write([]byte(", "))
		}
		code.Write([]byte(param.Text()))
	}
	code.Write([]byte(")\n"))

	r.BlockCode(buf, code.Bytes(), tickLang)

	buf.Write([]byte("\n<a href=\"javascript:document.getElementsByClassName('article')[0].scrollIntoView();\" title=\"top\">^</a>\n"))

	if len(p.EmbeddedProperties) > 0 {
		renderProperties(buf, r, p.EmbeddedProperties, nodes, header+1, code.String()+"      ", p.Name+" ")
	}

	return nil
}

func (p *Property) RenderAsRow(buf *bytes.Buffer, nodes map[string]*Node) error {

	buf.Write([]byte("| "))
	buf.Write([]byte(fmt.Sprintf("**[%s](#%s)&nbsp;(&nbsp;", nameToTickName(p.Name), strings.ToLower(p.Name))))

	for i, param := range p.Params {
		buf.Write([]byte(fmt.Sprintf("`%s`&nbsp;`%s`", param.Name, param.Type)))
		if (i + 1) < len(p.Params) {
			buf.Write([]byte(",&nbsp;"))
		}
	}

	buf.Write([]byte(")**"))

	buf.Write([]byte(" | "))
	if p.Doc != nil && len(p.Doc.List) > 0 {
		for _, line := range p.Doc.List {
			//Use only first paragraph - so stop on empty line
			if len(strings.TrimSpace(strings.TrimLeft(fmt.Sprint(line.Text), "/"))) == 0 {
				break
			}
			buf.Write([]byte(strings.TrimSpace(strings.TrimLeft(fmt.Sprint(line.Text), "/"))))
			buf.Write([]byte(" "))
		}
	}
	buf.Write([]byte(" |\n"))

	return nil
}

type Method struct {
	Name   string
	Doc    *ast.CommentGroup
	Params []Param
	Result string
}

func (m *Method) Render(buf *bytes.Buffer, r Renderer, nodes map[string]*Node, node string) error {
	r.Header(buf, func() bool { buf.Write([]byte(m.Name)); return true }, 3, "")

	renderDoc(buf, nodes, r, m.Doc)

	var code bytes.Buffer
	code.Write([]byte(node))
	code.Write([]byte("|"))
	code.Write([]byte(nameToTickName(m.Name)))
	code.Write([]byte("("))
	for i, param := range m.Params {
		if i != 0 {
			code.Write([]byte(", "))
		}
		code.Write([]byte(param.Text()))
	}
	code.Write([]byte(")\n"))

	r.BlockCode(buf, code.Bytes(), tickLang)

	r.Paragraph(buf, func() bool {
		buf.Write([]byte("Returns: "))
		r.Link(buf, []byte(nodeNameToLink(m.Result)), nil, []byte(m.Result))
		return true
	})

	return nil
}

type Param struct {
	Name string
	Type string
}

func (p Param) Text() string {
	return fmt.Sprintf("%s %s", p.Name, p.Type)
}

type Renderer interface {
	// block-level callbacks
	BlockCode(out *bytes.Buffer, text []byte, lang string)
	BlockQuote(out *bytes.Buffer, text []byte)
	BlockHtml(out *bytes.Buffer, text []byte)
	Header(out *bytes.Buffer, text func() bool, level int, id string)
	HRule(out *bytes.Buffer)
	List(out *bytes.Buffer, text func() bool, flags int)
	ListItem(out *bytes.Buffer, text []byte, flags int)
	Paragraph(out *bytes.Buffer, text func() bool)
	Table(out *bytes.Buffer, header []byte, body []byte, columnData []int)
	TableRow(out *bytes.Buffer, text []byte)
	TableHeaderCell(out *bytes.Buffer, text []byte, flags int)
	TableCell(out *bytes.Buffer, text []byte, flags int)
	Footnotes(out *bytes.Buffer, text func() bool)
	FootnoteItem(out *bytes.Buffer, name, text []byte, flags int)
	TitleBlock(out *bytes.Buffer, text []byte)

	// Span-level callbacks
	AutoLink(out *bytes.Buffer, link []byte, kind int)
	CodeSpan(out *bytes.Buffer, text []byte)
	DoubleEmphasis(out *bytes.Buffer, text []byte)
	Emphasis(out *bytes.Buffer, text []byte)
	Image(out *bytes.Buffer, link []byte, title []byte, alt []byte)
	LineBreak(out *bytes.Buffer)
	Link(out *bytes.Buffer, link []byte, title []byte, content []byte)
	RawHtmlTag(out *bytes.Buffer, tag []byte)
	TripleEmphasis(out *bytes.Buffer, text []byte)
	StrikeThrough(out *bytes.Buffer, text []byte)
	FootnoteRef(out *bytes.Buffer, ref []byte, id int)

	// Low-level callbacks
	Entity(out *bytes.Buffer, entity []byte)
	NormalText(out *bytes.Buffer, text []byte)

	// Header and footer
	DocumentHeader(out *bytes.Buffer)
	DocumentFooter(out *bytes.Buffer)

	GetFlags() int
}
