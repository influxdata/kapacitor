package main

import "text/template"

type Config struct {
	Root               string         `toml:"root"`
	PageHeader         string         `toml:"page-header"`
	IndexWidth         int            `toml:"index-width"`
	Weights            map[string]int `toml:"weights"`
	ChainMethodDesc    string         `toml:"chain-method-desc"`
	PropertyMethodDesc string         `toml:"property-method-desc"`

	headerTemplate *template.Template
}
