package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/influxdata/kapacitor/tick"
)

const backupExt = ".orig"

var writeFlag = flag.Bool("w", false, "write formatted contents to source file instead of STDOUT.")
var backupFlag = flag.Bool("b", false, fmt.Sprintf("create backup files with extension '%s'.", backupExt))

func usage() {
	message := `Usage: %s [options] [path...]

    If no source files are provided reads from STDIN.

Options:
`
	fmt.Fprintf(os.Stderr, message, os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Usage = usage
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		if *writeFlag {
			fmt.Fprintln(os.Stderr, "Cannot write source files, none given.")
			flag.Usage()
			os.Exit(2)
		}
		args = []string{"-"}
	}
	for _, path := range args {
		path = filepath.Clean(path)
		err := formatFile(path, *writeFlag, *backupFlag)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func formatFile(filename string, write, backup bool) error {
	data, err := readFile(filename)
	if err != nil {
		return err
	}
	formatted, err := tick.Format(data)
	if err != nil {
		return err
	}
	if write {
		dir := filepath.Dir(filename)
		tmp, err := writeTmpFile(dir, formatted)
		if err != nil {
			return err
		}
		defer os.Remove(tmp)
		if backup {
			err := os.Rename(filename, filename+backupExt)
			if err != nil {
				return err
			}
		}
		err = os.Rename(tmp, filename)
		if err != nil {
			return err
		}
	} else {
		_, err := os.Stdout.Write([]byte(formatted))
		if err != nil {
			return err
		}
	}
	return nil
}

func readFile(filename string) (string, error) {
	var f *os.File
	if filename == "-" {
		f = os.Stdin
	} else {
		var err error
		f, err = os.Open(filename)
		if err != nil {
			return "", nil
		}
	}
	defer f.Close()
	data, err := ioutil.ReadAll(f)
	if err != nil {
		return "", nil
	}
	return string(data), nil
}

func writeTmpFile(dir, contents string) (string, error) {
	f, err := ioutil.TempFile(dir, "tickfmt")
	if err != nil {
		return "", err
	}
	defer f.Close()
	_, err = f.Write([]byte(contents))
	return f.Name(), err
}
