package ast

import "errors"

// Walk calls f on all nodes reachable from the root node.
// The node returned will replace the node provided within the AST.
// Returning an error from f will stop the walking process and f will not be called on any other nodes.
func Walk(root Node, f func(n Node) (Node, error)) (Node, error) {
	replacement, err := f(root)
	if err != nil {
		return nil, err
	}
	switch node := replacement.(type) {
	case *LambdaNode:
		r, err := Walk(node.Expression, f)
		if err != nil {
			return nil, err
		}
		node.Expression = r
	case *UnaryNode:
		r, err := Walk(node.Node, f)
		if err != nil {
			return nil, err
		}
		node.Node = r
	case *BinaryNode:
		r, err := Walk(node.Left, f)
		if err != nil {
			return nil, err
		}
		node.Left = r
		r, err = Walk(node.Right, f)
		if err != nil {
			return nil, err
		}
		node.Right = r
	case *DeclarationNode:
		r, err := Walk(node.Left, f)
		if err != nil {
			return nil, err
		}
		ident, ok := r.(*IdentifierNode)
		if !ok {
			return nil, errors.New("declaration node must always have an IdentifierNode")
		}
		node.Left = ident
		r, err = Walk(node.Right, f)
		if err != nil {
			return nil, err
		}
		node.Right = r
	case *FunctionNode:
		for i := range node.Args {
			r, err := Walk(node.Args[i], f)
			if err != nil {
				return nil, err
			}
			node.Args[i] = r
		}
	case *ProgramNode:
		for i := range node.Nodes {
			r, err := Walk(node.Nodes[i], f)
			if err != nil {
				return nil, err
			}
			node.Nodes[i] = r
		}
	}
	return replacement, nil
}
