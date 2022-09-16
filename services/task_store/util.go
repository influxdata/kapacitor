package task_store

import (
	"errors"
	"fmt"

	client "github.com/influxdata/kapacitor/client/v1"
	"github.com/influxdata/kapacitor/tick/ast"
)

func newProgramNodeFromTickscript(tickscript string) (*ast.ProgramNode, error) {
	p, err := ast.Parse(tickscript)

	if err != nil {
		return nil, fmt.Errorf("invalid TICKscript: %v", err)
	}

	pn, ok := p.(*ast.ProgramNode)
	// This should never happen
	if !ok {
		return nil, errors.New("invalid TICKscript")
	}

	return pn, nil
}

func dbrpsFromProgram(n *ast.ProgramNode) []client.DBRP {
	var dbrps []client.DBRP
	for _, nn := range n.Nodes {
		switch nn := nn.(type) {
		case *ast.DBRPNode:
			dbrpc := client.DBRP{
				Database:        nn.DB.Reference,
				RetentionPolicy: nn.RP.Reference,
			}
			dbrps = append(dbrps, dbrpc)
		default:
			continue
		}
	}

	return dbrps
}

func taskTypeFromProgram(n *ast.ProgramNode) client.TaskType {
	tts := []string{}
	for _, nn := range n.Nodes {
		switch node := nn.(type) {
		case *ast.DeclarationNode:
			n := node.Right
		DeclLoop:
			for {
				switch nInner := n.(type) {
				case *ast.ChainNode:
					n = nInner.Left
				case *ast.IdentifierNode:
					if ident := nInner.Ident; ident == "batch" || ident == "stream" {
						tts = append(tts, ident)
					}
					break DeclLoop
				default:
					break DeclLoop
				}
			}
		case *ast.ChainNode:
			n := node.Left
		ChainLoop:
			for {
				switch nInner := n.(type) {
				case *ast.ChainNode:
					n = nInner.Left
				case *ast.IdentifierNode:
					if ident := nInner.Ident; ident == "batch" || ident == "stream" {
						tts = append(tts, ident)
					}
					break ChainLoop
				default:
					// Something went wrong, break out of loop.
					break ChainLoop
				}
			}
		}
	}

	if len(tts) == 0 {
		return client.InvalidTask
	}

	t := tts[0]
	for _, tt := range tts[1:] {
		if t != tt {
			return client.InvalidTask
		}
	}

	switch t {
	case "batch":
		return client.BatchTask
	case "stream":
		return client.StreamTask
	}

	return client.InvalidTask
}
