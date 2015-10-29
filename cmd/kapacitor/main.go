package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/influxdb/kapacitor"
)

// These variables are populated via the Go linker.
var (
	version string = "v0.1"
	commit  string
	branch  string
)

var mainFlags = flag.NewFlagSet("main", flag.ExitOnError)
var kapacitordURL = mainFlags.String("url", "http://localhost:9092/api/v1", "the URL http(s)://host:port of the kapacitord server.")

var l = log.New(os.Stderr, "[run] ", log.LstdFlags)

var usageStr = `
Usage: kapacitor [options] [command] [args]

Commands:

	record   record the result of a query or a snapshot of the current stream data.
	define   create/update a task.
	replay   replay a recording to a task.
	enable   enable and start running a task with live data.
	disable  stop running a task.
	reload   reload a running task with an updated task definition.
	push     publish a task definition to another Kapacitor instance. Not implemented yet.
	delete   delete a task or a recording.
	list     list information about tasks or recordings.
	help     get help for a command.
	level    sets the logging level on the kapacitord server.
	version  displays the Kapacitor version info.

Options:
`

func usage() {
	fmt.Fprintln(os.Stderr, usageStr)
	mainFlags.PrintDefaults()
	os.Exit(1)
}

func main() {

	mainFlags.Parse(os.Args[1:])

	args := mainFlags.Args()

	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "Error: Must pass a command.")
		usage()
	}

	command := args[0]
	args = args[1:]
	var commandF func(args []string) error
	var commandArgs []string
	switch command {
	case "help":
		commandArgs = args
		commandF = doHelp
	case "record":
		if len(args) == 0 {
			recordFlags.Usage()
			os.Exit(2)
		}
		recordFlags.Parse(args[1:])
		commandArgs = args[0:1]
		commandF = doRecord
	case "define":
		defineFlags.Parse(args)
		commandArgs = defineFlags.Args()
		commandF = doDefine
	case "replay":
		replayFlags.Parse(args)
		commandArgs = replayFlags.Args()
		commandF = doReplay
	case "enable":
		commandArgs = args
		commandF = doEnable
	case "disable":
		commandArgs = args
		commandF = doDisable
	case "reload":
		commandArgs = args
		commandF = doReload
	case "delete":
		commandArgs = args
		commandF = doDelete
	case "list":
		commandArgs = args
		commandF = doList
	case "level":
		commandArgs = args
		commandF = doLevel
	case "version":
		commandArgs = args
		commandF = doVersion
	default:
		fmt.Fprintln(os.Stderr, "Unknown command", command)
		usage()
	}

	err := commandF(commandArgs)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(3)
	}
}

// Init flag sets
func init() {
	replayFlags.Usage = replayUsage
	defineFlags.Usage = defineUsage
	recordFlags.Usage = recordUsage
}

// helper methods

type responseError struct {
	Err string `json:"Error"`
}

func (e responseError) Error() string {
	return e.Err
}

// Help

func helpUsage() {
	var u = "Usage: kapacitor help [command]\n"
	fmt.Fprintln(os.Stderr, u)
}

func doHelp(args []string) error {
	if len(args) == 1 {
		command := args[0]
		switch command {
		case "record":
			recordFlags.Usage()
		case "define":
			defineFlags.Usage()
		case "replay":
			replayFlags.Usage()
		case "enable":
			enableUsage()
		case "disable":
			disableUsage()
		case "reload":
			reloadUsage()
		case "delete":
			deleteUsage()
		case "list":
			listUsage()
		case "level":
			levelUsage()
		case "help":
			helpUsage()
		case "version":
			versionUsage()
		default:
			fmt.Fprintln(os.Stderr, "Unknown command", command)
			usage()
		}
	} else {
		helpUsage()
		usage()
	}
	return nil
}

// Record
var (
	recordFlags = flag.NewFlagSet("record", flag.ExitOnError)
	rname       = recordFlags.String("name", "", "the name of a task. If recording a batch or stream")

	rstart = recordFlags.String("start", "", "the start time for the set of queries when recording a batch.")
	rpast  = recordFlags.String("past", "", "set start time via 'now - past'.")
	rnum   = recordFlags.Int("num", 0, "the number of periods to query, if zero will query as many times as the schedule defines until the queries reach the present time. Applies only to recording a batch.")

	rquery = recordFlags.String("query", "", "the query to record. If recording a query.")
	rtype  = recordFlags.String("type", "", "the type of the recording to save (stream|batch). If recording a query.")

	rdur = recordFlags.String("duration", "", "how long to record the data stream. If recording a stream.")
)

func recordUsage() {
	var u = `Usage: kapacitor record [batch|stream|query] [options]

	Record the result of a InfluxDB query or a snapshot of the live data stream.

	Prints the recording ID on exit.

	Recordings have types like tasks. If recording a raw query you must specify the desired type.

	See 'kapacitor help replay' for how to replay a recording.

Examples:

	$ kapacitor record stream -name mem_free -duration 1m

		This records the live data stream for 1 minute using the databases and retention policies
		from the named task.
	
	$ kapacitor record batch -name cpu_idle -start 2015-09-01T00:00:00Z -num 10
		
		This records the result of the query defined in task 'cpu_idle' and runs the query 10 times
		starting at time 'start' and incrementing by the schedule defined in the task.

	$ kapacitor record batch -name cpu_idle -past 10h
		
		This records the result of the query defined in task 'cpu_idle' and runs the query
		as many times as defined by the schedule until the queries reaches the present time.
		The starting time for the queries is 'now - 10h' and increments by the schedule defined in the task.

	$ kapacitor record query -query "select value from cpu_idle where time > now() - 1h and time < now()" -type stream

		This records the result of the query and stores it as a stream recording. Use -type batch to store as batch recording.

Options:
`
	fmt.Fprintln(os.Stderr, u)
	recordFlags.PrintDefaults()
}

func doRecord(args []string) error {

	v := url.Values{}
	v.Add("type", args[0])
	switch args[0] {
	case "stream":
		v.Add("name", *rname)
		v.Add("duration", *rdur)
	case "batch":
		if *rstart != "" && *rpast != "" {
			return errors.New("cannot set both start and past flags.")
		}
		v.Add("name", *rname)
		v.Add("start", *rstart)
		v.Add("past", *rpast)
		v.Add("num", strconv.FormatInt(int64(*rnum), 10))
	case "query":
		v.Add("qtype", *rtype)
	default:
		return fmt.Errorf("Unknown record type %q, expected 'stream' or 'query'", args[0])
	}
	r, err := http.Post(*kapacitordURL+"/record?"+v.Encode(), "application/octetstream", nil)
	if err != nil {
		return err
	}
	defer r.Body.Close()

	// Decode valid response
	type resp struct {
		RecordingID string `json:"RecordingID"`
		Error       string `json:"Error"`
	}
	d := json.NewDecoder(r.Body)
	rp := resp{}
	d.Decode(&rp)
	if rp.Error != "" {
		return errors.New(rp.Error)
	}
	fmt.Println(rp.RecordingID)
	return nil
}

// Define
var (
	defineFlags = flag.NewFlagSet("define", flag.ExitOnError)
	dname       = defineFlags.String("name", "", "the task name")
	dtick       = defineFlags.String("tick", "", "path to the TICKscript")
	dtype       = defineFlags.String("type", "", "the task type (stream|batch)")
	ddbrp       = make(dbrps, 0)
)

func init() {
	defineFlags.Var(&ddbrp, "dbrp", `a database and retention policy pair of the form "db"."rp" the quotes are optional. The flag can be specified multiple times.`)
}

type dbrps []kapacitor.DBRP

func (d *dbrps) String() string {
	return fmt.Sprint(*d)
}

// Parse string of the form "db"."rp" where the quotes are optional but can include escaped quotes
// within the strings.
func (d *dbrps) Set(value string) error {
	dbrp := kapacitor.DBRP{}
	if len(value) == 0 {
		return fmt.Errorf("dbrp cannot be empty")
	}
	var n int
	if value[0] == '"' {
		dbrp.Database, n = parseQuotedStr(value)
	} else {
		n = strings.IndexRune(value, '.')
		dbrp.Database = value[:n]
	}
	if value[n] != '.' {
		return fmt.Errorf("dbrp must specify retention policy, do you have a missing or extra '.'?")
	}
	value = value[n+1:]
	if value[0] == '"' {
		dbrp.RetentionPolicy, n = parseQuotedStr(value)
	} else {
		dbrp.RetentionPolicy = value
	}
	*d = append(*d, dbrp)
	return nil
}

// read from txt starting with begining quote until next unescaped quote.
func parseQuotedStr(txt string) (string, int) {
	literal := txt[1 : len(txt)-1]
	quote := txt[0]
	// Unescape quotes
	var buf bytes.Buffer
	buf.Grow(len(literal))
	last := 0
	i := 0
	for ; i < len(literal)-1; i++ {
		if literal[i] == '\\' && literal[i+1] == quote {
			buf.Write([]byte(literal[last:i]))
			buf.Write([]byte{quote})
			i += 2
			last = i
		} else if literal[i] == quote {
			break
		}
	}
	buf.Write([]byte(literal[last:i]))
	literal = buf.String()
	return literal, i + 1
}

func defineUsage() {
	var u = `Usage: kapacitor define [options]

Create or update a task.

A task is defined via a TICKscript that defines the data processing pipeline of the task.

If an option is absent it will be left unmodified.

For example:

    You can define a task for the first time with all the flags.

    $ kapacitor define -name my_task -tick path/to/TICKscript -type stream -dbrp mydb.myrp

    Later you can change a sinlge property of the task by referencing its name 
    and only providing the single option you wish to modify.

    $ kapacitor define -name my_task -tick path/to/TICKscript

    or

    $ kapacitor define -name my_task -dbrp mydb.myrp -dbrp otherdb.default

    NOTE: you must specify all 'dbrp' flags you desire if you wish to modify them.

Options:

`
	fmt.Fprintln(os.Stderr, u)
	defineFlags.PrintDefaults()
}

func doDefine(args []string) error {

	if *dname == "" {
		fmt.Fprintln(os.Stderr, "Must always pass name flag.")
		defineFlags.Usage()
		os.Exit(2)
	}

	var f io.Reader
	if *dtick != "" {
		var err error
		f, err = os.Open(*dtick)
		if err != nil {
			return err
		}
	}
	v := url.Values{}
	v.Add("name", *dname)
	v.Add("type", *dtype)
	if len(ddbrp) > 0 {
		b, err := json.Marshal(ddbrp)
		if err != nil {
			return err
		}
		v.Add("dbrps", string(b))
	}
	r, err := http.Post(*kapacitordURL+"/task?"+v.Encode(), "application/octetstream", f)
	if err != nil {
		return err
	}
	defer r.Body.Close()

	// Decode valid response
	type resp struct {
		Error string `json:"Error"`
	}
	d := json.NewDecoder(r.Body)
	rp := resp{}
	d.Decode(&rp)
	if rp.Error != "" {
		return errors.New(rp.Error)
	}
	return nil
}

// Replay
var (
	replayFlags = flag.NewFlagSet("replay", flag.ExitOnError)
	rtname      = replayFlags.String("name", "", "the task name")
	rid         = replayFlags.String("id", "", "the recording ID")
	rfast       = replayFlags.Bool("fast", false, "If set, replay the data as fast as possible. If not set, replay the data in real time.")
)

func replayUsage() {
	var u = `Usage: kapacitor replay [options]

Replay a recording to a task. Waits until the task finishes.

See 'kapacitor help record' for how to create a replay.
See 'kapacitor help define' for how to create a task.

Options:
`
	fmt.Fprintln(os.Stderr, u)
	replayFlags.PrintDefaults()
}

func doReplay(args []string) error {

	v := url.Values{}
	v.Add("name", *rtname)
	v.Add("id", *rid)
	if *rfast {
		v.Add("clock", "fast")
	}
	r, err := http.Post(*kapacitordURL+"/replay?"+v.Encode(), "application/octetstream", nil)
	if err != nil {
		return err
	}
	defer r.Body.Close()

	// Decode valid response
	type resp struct {
		Error string `json:"Error"`
	}
	d := json.NewDecoder(r.Body)
	rp := resp{}
	d.Decode(&rp)
	if rp.Error != "" {
		return errors.New(rp.Error)
	}
	return nil
}

// Enable
func enableUsage() {
	var u = `Usage: kapacitor enable [task name...]

	Enable and start a task running from the live data.
`
	fmt.Fprintln(os.Stderr, u)
}

func doEnable(args []string) error {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Must pass at least one task name")
		enableUsage()
		os.Exit(2)
	}

	for _, name := range args {
		v := url.Values{}
		v.Add("name", name)
		r, err := http.Post(*kapacitordURL+"/enable?"+v.Encode(), "application/octetstream", nil)
		if err != nil {
			return err
		}
		defer r.Body.Close()
		// Decode valid response
		type resp struct {
			Error string `json:"Error"`
		}
		d := json.NewDecoder(r.Body)
		rp := resp{}
		d.Decode(&rp)
		if rp.Error != "" {
			return errors.New(rp.Error)
		}
	}
	return nil
}

// Disable

func disableUsage() {
	var u = `Usage: kapacitor disable [task name...]

	Disable and stop a task running.
`
	fmt.Fprintln(os.Stderr, u)
}

func doDisable(args []string) error {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Must pass at least one task name")
		disableUsage()
		os.Exit(2)
	}

	for _, name := range args {
		v := url.Values{}
		v.Add("name", name)
		r, err := http.Post(*kapacitordURL+"/disable?"+v.Encode(), "application/octetstream", nil)
		if err != nil {
			return err
		}
		defer r.Body.Close()
		// Decode valid response
		type resp struct {
			Error string `json:"Error"`
		}
		d := json.NewDecoder(r.Body)
		rp := resp{}
		d.Decode(&rp)
		if rp.Error != "" {
			return errors.New(rp.Error)
		}
		return nil
	}
	return nil
}

// Reload

func reloadUsage() {
	var u = `Usage: kapacitor reload [task name...]

	Disable then enable a running task.
`
	fmt.Fprintln(os.Stderr, u)
}

func doReload(args []string) error {
	err := doDisable(args)
	if err != nil {
		return err
	}

	return doEnable(args)
}

// List

func listUsage() {
	var u = `Usage: kapacitor list (tasks|recordings) [task name|recording ID]...

List tasks or recordings and their current state.

If no tasks are given then all tasks are listed. Same for recordings.
If a set of task names or recordings IDs is provided only those entries will be listed.
`
	fmt.Fprintln(os.Stderr, u)
}

func doList(args []string) error {

	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "Must specify 'tasks' or 'recordings'")
		listUsage()
		os.Exit(2)
	}

	switch kind := args[0]; kind {
	case "tasks":
		tasks := strings.Join(args[1:], ",")
		v := url.Values{}
		v.Add("tasks", tasks)
		r, err := http.Get(*kapacitordURL + "/tasks?" + v.Encode())
		if err != nil {
			return err
		}
		defer r.Body.Close()
		// Decode valid response
		type resp struct {
			Error string `json:"Error"`
			Tasks []struct {
				Name    string
				Type    kapacitor.TaskType
				DBRPs   []kapacitor.DBRP
				Enabled bool
			} `json:"Tasks"`
		}
		d := json.NewDecoder(r.Body)
		rp := resp{}
		d.Decode(&rp)
		if rp.Error != "" {
			return errors.New(rp.Error)
		}

		outFmt := "%-30s%-10v%-10v%s\n"
		fmt.Fprintf(os.Stdout, outFmt, "Name", "Type", "Enabled", "Databases and Retention Policies")
		for _, t := range rp.Tasks {
			fmt.Fprintf(os.Stdout, outFmt, t.Name, t.Type, t.Enabled, t.DBRPs)
		}
	case "recordings":

		rids := strings.Join(args[1:], ",")
		v := url.Values{}
		v.Add("rids", rids)
		r, err := http.Get(*kapacitordURL + "/recordings?" + v.Encode())
		if err != nil {
			return err
		}
		defer r.Body.Close()
		// Decode valid response
		type resp struct {
			Error      string `json:"Error"`
			Recordings []struct {
				ID   string
				Type kapacitor.TaskType
				Size int64
			} `json:"Recordings"`
		}
		d := json.NewDecoder(r.Body)
		rp := resp{}
		d.Decode(&rp)
		if rp.Error != "" {
			return errors.New(rp.Error)
		}

		outFmt := "%-40s%-10v%15s\n"
		fmt.Fprintf(os.Stdout, "%-40s%-10s%15s\n", "ID", "Type", "Size")
		for _, r := range rp.Recordings {
			fmt.Fprintf(os.Stdout, outFmt, r.ID, r.Type, humanize.Bytes(uint64(r.Size)))
		}
	default:
		return fmt.Errorf("cannot list '%s' did you mean 'tasks' or 'recordings'?", kind)
	}
	return nil

}

// Delete
func deleteUsage() {
	var u = `Usage: kapacitor delete (tasks|recordings) [task name|recording ID]...

	Delete a task or recording.
	
	If a task is enabled it will be disabled and then deleted.
`
	fmt.Fprintln(os.Stderr, u)
}

func doDelete(args []string) error {
	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, "Must pass at least one task name or recording ID")
		enableUsage()
		os.Exit(2)
	}

	var baseURL string
	var paramName string
	switch kind := args[0]; kind {
	case "tasks":
		baseURL = *kapacitordURL + "/task?"
		paramName = "name"
	case "recordings":
		baseURL = *kapacitordURL + "/recording?"
		paramName = "rid"
	default:
		return fmt.Errorf("cannot delete '%s' did you mean 'tasks' or 'recordings'?", kind)
	}

	for _, arg := range args[1:] {
		v := url.Values{}
		v.Add(paramName, arg)
		req, err := http.NewRequest("DELETE", baseURL+v.Encode(), nil)
		if err != nil {
			return err
		}
		client := &http.Client{}
		r, err := client.Do(req)
		if err != nil {
			return err
		}
		defer r.Body.Close()
		// Decode valid response
		type resp struct {
			Error string `json:"Error"`
		}
		d := json.NewDecoder(r.Body)
		rp := resp{}
		d.Decode(&rp)
		if rp.Error != "" {
			return errors.New(rp.Error)
		}
	}

	return nil
}

// Level
func levelUsage() {
	var u = `Usage: kapacitor level (debug|info|warn|error)

	Sets the logging level on the kapacitord server.
`
	fmt.Fprintln(os.Stderr, u)
}

func doLevel(args []string) error {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "Must pass a log level")
		levelUsage()
		os.Exit(2)
	}
	v := url.Values{}
	v.Add("level", args[0])
	r, err := http.Post(*kapacitordURL+"/loglevel?"+v.Encode(), "text/plain", nil)
	if err != nil {
		return err
	}
	defer r.Body.Close()
	// Decode valid response
	type resp struct {
		Error string `json:"Error"`
	}
	d := json.NewDecoder(r.Body)
	rp := resp{}
	d.Decode(&rp)
	if rp.Error != "" {
		return errors.New(rp.Error)
	}
	return nil
}

// Version
func versionUsage() {
	var u = `Usage: kapacitor version

	Print version info.
`
	fmt.Fprintln(os.Stderr, u)
}

func doVersion(args []string) error {
	fmt.Fprintf(os.Stdout, "Kapacitor %s (git: %s %s)\n", version, branch, commit)
	return nil
}
