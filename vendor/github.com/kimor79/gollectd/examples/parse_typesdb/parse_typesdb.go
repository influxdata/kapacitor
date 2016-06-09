package main

import (
	"flag"
	"fmt"
	"os"

	collectd "github.com/kimor79/gollectd"
)

func main() {
	typesPath := flag.String("typesdb", "", "Path to types.db")
	flag.Parse()

	if *typesPath == "" {
		flag.Usage()
		os.Exit(1)
	}

	types, err := collectd.TypesDBFile(*typesPath)

	if err != nil {
		fmt.Printf("%s\n", err)
		os.Exit(1)
	}

	for k, pv := range types {
		fmt.Printf("%s:\n", k)
		for _, v := range pv {
			fmt.Printf("\t%+v\n", *v)
		}
	}
}
