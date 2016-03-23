package tick

import (
	"fmt"
)

type Process struct {
	Name     string
	Children []*Process
}

func (p *Process) Spawn() *Process {
	child := &Process{}
	p.Children = append(p.Children, child)
	return child
}

func (p *Process) String() string {
	return fmt.Sprintf("{%q %s}", p.Name, p.Children)
}

func ExampleEvaluate() {

	//Run a test that evaluates the DSL against the Process struct.
	script := `
//Name the parent
parent.name('parent')

// Spawn a first child
var child1 = parent|spawn()

// Name the first child
child1.name('child1')

//Spawn a grandchild and name it
child1|spawn().name('grandchild')

//Spawn a second child and name it
parent|spawn().name('child2')
`

	scope := NewScope()
	parent := &Process{}
	scope.Set("parent", parent)

	err := Evaluate(script, scope)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(parent)
	// Output: {"parent" [{"child1" [{"grandchild" []}]} {"child2" []}]}
}
