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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/influxdata/kapacitor"
	"github.com/influxdata/kapacitor/services/replay"
	"github.com/influxdata/kapacitor/services/task_store"
)

// These variables are populated via the Go linker.
var (
	version string
	commit  string
	branch  string
)

var defaultURL = "http://localhost:9092"

var mainFlags = flag.NewFlagSet("main", flag.ExitOnError)
var kapacitordURL = mainFlags.String("url", "", "the URL http(s)://host:port of the kapacitord server. Defaults to the KAPACITOR_URL environment variable or "+defaultURL+" if not set.")

var kapacitorEndpoint string

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
	show     display detailed information about a task.
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

	url := defaultURL
	if urlEnv := os.Getenv("KAPACITOR_URL"); urlEnv != "" {
		url = urlEnv
	}
	if *kapacitordURL != "" {
		url = *kapacitordURL
	}

	kapacitorEndpoint = url

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
			recordUsage()
			os.Exit(2)
		}
		commandArgs = args
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
	case "show":
		commandArgs = args
		commandF = doShow
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
	recordStreamFlags.Usage = recordStreamUsage
	recordBatchFlags.Usage = recordBatchUsage
	recordQueryFlags.Usage = recordQueryUsage
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
			recordUsage()
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
		case "show":
			showUsage()
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
	recordStreamFlags = flag.NewFlagSet("record-stream", flag.ExitOnError)
	rsname            = recordStreamFlags.String("name", "", "the name of a task. Uses the dbrp value for the task.")
	rsdur             = recordStreamFlags.String("duration", "", "how long to record the data stream.")

	recordBatchFlags = flag.NewFlagSet("record-batch", flag.ExitOnError)
	rbname           = recordBatchFlags.String("name", "", "the name of a task. Uses the queries contained in the task.")
	rbstart          = recordBatchFlags.String("start", "", "the start time for the set of queries.")
	rbstop           = recordBatchFlags.String("stop", "", "the stop time for the set of queries (default now).")
	rbpast           = recordBatchFlags.String("past", "", "set start time via 'now - past'.")
	rbcluster        = recordBatchFlags.String("cluster", "", "optional named InfluxDB cluster from configuration.")

	recordQueryFlags = flag.NewFlagSet("record-query", flag.ExitOnError)
	rqquery          = recordQueryFlags.String("query", "", "the query to record.")
	rqtype           = recordQueryFlags.String("type", "", "the type of the recording to save (stream|batch).")
	rqcluster        = recordQueryFlags.String("cluster", "", "optional named InfluxDB cluster from configuration.")
)

func recordUsage() {
	var u = `Usage: kapacitor record [batch|stream|query] [options]

	Record the result of a InfluxDB query or a snapshot of the live data stream.

	Prints the recording ID on exit.

	See 'kapacitor help replay' for how to replay a recording.
`
	fmt.Fprintln(os.Stderr, u)
}

func recordStreamUsage() {
	var u = `Usage: kapacitor record stream [options]

	Record a snapshot of the live data stream.

	Prints the recording ID on exit.

	See 'kapacitor help replay' for how to replay a recording.

Examples:

	$ kapacitor record stream -name mem_free -duration 1m

		This records the live data stream for 1 minute using the databases and retention policies
		from the named task.

Options:
`
	fmt.Fprintln(os.Stderr, u)
	recordStreamFlags.PrintDefaults()
}

func recordBatchUsage() {
	var u = `Usage: kapacitor record batch [options]

	Record the result of a InfluxDB query from a task.

	Prints the recording ID on exit.

	See 'kapacitor help replay' for how to replay a recording.

Examples:

	$ kapacitor record batch -name cpu_idle -start 2015-09-01T00:00:00Z -stop 2015-09-02T00:00:00Z

		This records the result of the query defined in task 'cpu_idle' and runs the query as many times
		as many times as defined by the schedule until the queries reaches the stop time.
		starting at time 'start' and incrementing by the schedule defined in the task.

	$ kapacitor record batch -name cpu_idle -past 10h

		This records the result of the query defined in task 'cpu_idle' and runs the query
		as many times as defined by the schedule until the queries reaches the present time.
		The starting time for the queries is 'now - 10h' and increments by the schedule defined in the task.

Options:
`
	fmt.Fprintln(os.Stderr, u)
	recordBatchFlags.PrintDefaults()
}

func recordQueryUsage() {
	var u = `Usage: kapacitor record query [options]

	Record the result of a InfluxDB query.

	Prints the recording ID on exit.

	Recordings have types like tasks. you must specify the desired type.

	See 'kapacitor help replay' for how to replay a recording.

Examples:

	$ kapacitor record query -query 'select value from "telegraf"."default"."cpu_idle" where time > now() - 1h and time < now()' -type stream

		This records the result of the query and stores it as a stream recording. Use '-type batch' to store as batch recording.

Options:
`
	fmt.Fprintln(os.Stderr, u)
	recordQueryFlags.PrintDefaults()
}
func doRecord(args []string) error {
	v := url.Values{}
	v.Add("type", args[0])
	switch args[0] {
	case "stream":
		recordStreamFlags.Parse(args[1:])
		if *rsname == "" || *rsdur == "" {
			recordStreamFlags.Usage()
			return errors.New("both name and duration are required")
		}
		v.Add("name", *rsname)
		v.Add("duration", *rsdur)
	case "batch":
		recordBatchFlags.Parse(args[1:])
		if *rbname == "" {
			recordBatchFlags.Usage()
			return errors.New("name is required")
		}
		if *rbstart == "" && *rbpast == "" {
			recordBatchFlags.Usage()
			return errors.New("must set one of start or past flags.")
		}
		if *rbstart != "" && *rbpast != "" {
			recordBatchFlags.Usage()
			return errors.New("cannot set both start and past flags.")
		}
		v.Add("name", *rbname)
		v.Add("start", *rbstart)
		v.Add("stop", *rbstop)
		v.Add("past", *rbpast)
		v.Add("cluster", *rbcluster)
	case "query":
		recordQueryFlags.Parse(args[1:])
		if *rqquery == "" || *rqtype == "" {
			recordQueryFlags.Usage()
			return errors.New("both query and type are required")
		}
		v.Add("query", *rqquery)
		v.Add("ttype", *rqtype)
		v.Add("cluster", *rqcluster)
	default:
		return fmt.Errorf("Unknown record type %q, expected 'stream', 'batch' or 'query'", args[0])
	}
	r, err := http.Post(kapacitorEndpoint+"/record?"+v.Encode(), "application/octetstream", nil)
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

	v = url.Values{}
	v.Add("id", rp.RecordingID)
	r, err = http.Get(kapacitorEndpoint + "/record?" + v.Encode())
	if err != nil {
		return err
	}
	defer r.Body.Close()

	d = json.NewDecoder(r.Body)
	ri := replay.RecordingInfo{}
	d.Decode(&ri)
	if ri.Error != "" {
		return errors.New(ri.Error)
	}

	fmt.Println(ri.ID)
	return nil
}

// Define
var (
	defineFlags = flag.NewFlagSet("define", flag.ExitOnError)
	dname       = defineFlags.String("name", "", "the task name")
	dtick       = defineFlags.String("tick", "", "path to the TICKscript")
	dtype       = defineFlags.String("type", "", "the task type (stream|batch)")
	dnoReload   = defineFlags.Bool("no-reload", false, "do not reload the task even if it is enabled")
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
		return errors.New("dbrp cannot be empty")
	}
	var n int
	if value[0] == '"' {
		dbrp.Database, n = parseQuotedStr(value)
	} else {
		n = strings.IndexRune(value, '.')
		if n == -1 {
			return errors.New("does not contain a '.', it must be in the form \"dbname\".\"rpname\" where the quotes are optional.")
		}
		dbrp.Database = value[:n]
	}
	if value[n] != '.' {
		return errors.New("dbrp must specify retention policy, do you have a missing or extra '.'?")
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

If the task is enabled then it will be reloaded unless -no-reload is specified.

For example:

    You can define a task for the first time with all the flags.

    $ kapacitor define -name my_task -tick path/to/TICKscript -type stream -dbrp mydb.myrp

    Later you can change a single property of the task by referencing its name
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
	r, err := http.Post(kapacitorEndpoint+"/task?"+v.Encode(), "application/octetstream", f)
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

	// Check if task is enabled if -no-reload was not given
	if !*dnoReload {
		v = url.Values{}
		v.Add("name", *dname)
		r, err = http.Get(kapacitorEndpoint + "/task?" + v.Encode())
		if err != nil {
			return err
		}
		defer r.Body.Close()
		d = json.NewDecoder(r.Body)
		ti := task_store.TaskInfo{}
		d.Decode(&ti)

		if ti.Name == "" && ti.Error != "" {
			return errors.New(ti.Error)
		}

		// Reload task if enabled.
		if ti.Enabled {
			return doReload([]string{*dname})
		}
	}

	return nil
}

// Replay
var (
	replayFlags = flag.NewFlagSet("replay", flag.ExitOnError)
	rtname      = replayFlags.String("name", "", "the task name")
	rid         = replayFlags.String("id", "", "the recording ID")
	rfast       = replayFlags.Bool("fast", false, "If set, replay the data as fast as possible. If not set, replay the data in real time.")
	rrec        = replayFlags.Bool("rec-time", false, "If set, use the times saved in the recording instead of present times.")
)

func replayUsage() {
	var u = `Usage: kapacitor replay [options]

Replay a recording to a task. Waits until the task finishes.

The times of the data points will either be relative to now or the exact times
in the recording if the '-rec-time' flag is set. In either case the relative times
between the data points remains the same.

See 'kapacitor help record' for how to create a replay.
See 'kapacitor help define' for how to create a task.

Options:
`
	fmt.Fprintln(os.Stderr, u)
	replayFlags.PrintDefaults()
}

func doReplay(args []string) error {
	if *rid == "" {
		return errors.New("must pass recording id")
	}
	if *rtname == "" {
		return errors.New("must pass task name")
	}

	v := url.Values{}
	v.Add("name", *rtname)
	v.Add("id", *rid)
	v.Add("rec-time", strconv.FormatBool(*rrec))
	if *rfast {
		v.Add("clock", "fast")
	}
	r, err := http.Post(kapacitorEndpoint+"/replay?"+v.Encode(), "application/octetstream", nil)
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

For example:

    You can enable by specific task name.

    $ kapacitor enable cpu_alert

		Or, you can enable by glob:

    $ kapacitor enable *_alert
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
		r, err := http.Post(kapacitorEndpoint+"/enable?"+v.Encode(), "application/octetstream", nil)
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

For example:

    You can disable by specific task name.

    $ kapacitor disable cpu_alert

		Or, you can disable by glob:

    $ kapacitor disable *_alert
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
		r, err := http.Post(kapacitorEndpoint+"/disable?"+v.Encode(), "application/octetstream", nil)
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

// Reload

func reloadUsage() {
	var u = `Usage: kapacitor reload [task name...]

	Disable then enable a running task.

For example:

    You can reload by specific task name.

    $ kapacitor reload cpu_alert

		Or, you can reload by glob:

    $ kapacitor reload *_alert
`
	fmt.Fprintln(os.Stderr, u)
}

func doReload(args []string) error {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Must pass at least one task name")
		reloadUsage()
		os.Exit(2)
	}
	err := doDisable(args)
	if err != nil {
		return err
	}

	return doEnable(args)
}

// Show

func showUsage() {
	var u = `Usage: kapacitor show [task name]

	Show details about a specific task.
`
	fmt.Fprintln(os.Stderr, u)
}

func doShow(args []string) error {

	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "Must specify one task name")
		showUsage()
		os.Exit(2)
	}

	v := url.Values{}
	v.Add("name", args[0])
	r, err := http.Get(kapacitorEndpoint + "/task?" + v.Encode())
	if err != nil {
		return err
	}
	defer r.Body.Close()
	d := json.NewDecoder(r.Body)
	ti := task_store.TaskInfo{}
	d.Decode(&ti)

	if ti.Name == "" && ti.Error != "" {
		return errors.New(ti.Error)
	}

	fmt.Println("Name:", ti.Name)
	fmt.Println("Error:", ti.Error)
	fmt.Println("Type:", ti.Type)
	fmt.Println("Enabled:", ti.Enabled)
	fmt.Println("Executing:", ti.Executing)
	fmt.Println("Databases Retention Policies:", ti.DBRPs)
	fmt.Printf("TICKscript:\n%s\n\n", ti.TICKscript)
	fmt.Printf("DOT:\n%s\n", ti.Dot)
	return nil
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
		r, err := http.Get(kapacitorEndpoint + "/tasks?" + v.Encode())
		if err != nil {
			return err
		}
		defer r.Body.Close()
		// Decode valid response
		type resp struct {
			Error string                       `json:"Error"`
			Tasks []task_store.TaskSummaryInfo `json:"Tasks"`
		}
		d := json.NewDecoder(r.Body)
		rp := resp{}
		d.Decode(&rp)
		if rp.Error != "" {
			return errors.New(rp.Error)
		}

		outFmt := "%-30s%-10v%-10v%-10v%s\n"
		fmt.Fprintf(os.Stdout, outFmt, "Name", "Type", "Enabled", "Executing", "Databases and Retention Policies")
		for _, t := range rp.Tasks {
			fmt.Fprintf(os.Stdout, outFmt, t.Name, t.Type, t.Enabled, t.Executing, t.DBRPs)
		}
	case "recordings":

		rids := strings.Join(args[1:], ",")
		v := url.Values{}
		v.Add("rids", rids)
		r, err := http.Get(kapacitorEndpoint + "/recordings?" + v.Encode())
		if err != nil {
			return err
		}
		defer r.Body.Close()
		// Decode valid response
		type resp struct {
			Error      string         `json:"Error"`
			Recordings recordingInfos `json:"Recordings"`
		}
		d := json.NewDecoder(r.Body)
		rp := resp{}
		d.Decode(&rp)
		if rp.Error != "" {
			return errors.New(rp.Error)
		}
		sort.Sort(rp.Recordings)

		outFmt := "%-40s%-8v%-10s%-23s\n"
		fmt.Fprintf(os.Stdout, "%-40s%-8s%-10s%-23s\n", "ID", "Type", "Size", "Created")
		for _, r := range rp.Recordings {
			fmt.Fprintf(os.Stdout, outFmt, r.ID, r.Type, humanize.Bytes(uint64(r.Size)), r.Created.Local().Format(time.RFC822))
		}
	default:
		return fmt.Errorf("cannot list '%s' did you mean 'tasks' or 'recordings'?", kind)
	}
	return nil

}

type recordingInfos []replay.RecordingInfo

func (r recordingInfos) Len() int           { return len(r) }
func (r recordingInfos) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r recordingInfos) Less(i, j int) bool { return r[i].Created.Before(r[j].Created) }

// Delete
func deleteUsage() {
	var u = `Usage: kapacitor delete (tasks|recordings) [task name|recording ID]...

	Delete a task or recording.

	If a task is enabled it will be disabled and then deleted,
For example:

		You can delete task:

    $ kapacitor delete tasks my_task

		Or you can delete tasks by glob:

    $ kapacitor delete tasks *_alert 

		You can delete recordings:

    $ kapacitor delete recordings b0a2ba8a-aeeb-45ec-bef9-1a2939963586 
`
	fmt.Fprintln(os.Stderr, u)
}

func doDelete(args []string) error {
	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, "Must pass at least one task name or recording ID")
		deleteUsage()
		os.Exit(2)
	}

	var baseURL string
	var paramName string
	switch kind := args[0]; kind {
	case "tasks":
		baseURL = kapacitorEndpoint + "/task?"
		paramName = "name"
	case "recordings":
		baseURL = kapacitorEndpoint + "/recording?"
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
	r, err := http.Post(kapacitorEndpoint+"/loglevel?"+v.Encode(), "text/plain", nil)
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
