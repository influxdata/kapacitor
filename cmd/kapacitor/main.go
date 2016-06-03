package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/kapacitor/client/v1"
	"github.com/pkg/errors"
)

// These variables are populated via the Go linker.
var (
	version string
	commit  string
	branch  string
)

var defaultURL = "http://localhost:9092"

var mainFlags = flag.NewFlagSet("main", flag.ExitOnError)
var kapacitordURL = mainFlags.String("url", "", "The URL http(s)://host:port of the kapacitord server. Defaults to the KAPACITOR_URL environment variable or "+defaultURL+" if not set.")

var l = log.New(os.Stderr, "[run] ", log.LstdFlags)

var cli *client.Client

var usageStr = `
Usage: kapacitor [options] [command] [args]

Commands:

	record          Record the result of a query or a snapshot of the current stream data.
	define          Create/update a task.
	define-template Create/update a template.
	replay          Replay a recording to a task.
	replay-live     Replay data against a task without recording it.
	enable          Enable and start running a task with live data.
	disable         Stop running a task.
	reload          Reload a running task with an updated task definition.
	push            Publish a task definition to another Kapacitor instance. Not implemented yet.
	delete          Delete tasks, templates, recordings or replays.
	list            List information about tasks, templates, recordings or replays.
	show            Display detailed information about a task.
	show-template   Display detailed information about a template.
	level           Sets the logging level on the kapacitord server.
	stats           Display various stats about Kapacitor.
	version         Displays the Kapacitor version info.
	vars            Print debug vars in JSON format.
	help            Prints help for a command.

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

	var err error
	cli, err = connect(url)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(4)
	}

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
		commandArgs = args
		commandF = doDefine
	case "define-template":
		commandArgs = args
		commandF = doDefineTemplate
	case "replay":
		replayFlags.Parse(args)
		commandArgs = replayFlags.Args()
		commandF = doReplay
	case "replay-live":
		if len(args) == 0 {
			replayLiveUsage()
			os.Exit(2)
		}
		commandArgs = args
		commandF = doReplayLive
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
	case "show-template":
		commandArgs = args
		commandF = doShowTemplate
	case "level":
		commandArgs = args
		commandF = doLevel
	case "stats":
		commandArgs = args
		commandF = doStats
	case "version":
		commandArgs = args
		commandF = doVersion
	case "vars":
		commandArgs = args
		commandF = doVars
	default:
		fmt.Fprintln(os.Stderr, "Unknown command", command)
		usage()
	}

	err = commandF(commandArgs)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(3)
	}
}

// Init flag sets
func init() {
	replayFlags.Usage = replayUsage
	defineFlags.Usage = defineUsage
	defineTemplateFlags.Usage = defineTemplateUsage

	recordStreamFlags.Usage = recordStreamUsage
	recordBatchFlags.Usage = recordBatchUsage
	recordQueryFlags.Usage = recordQueryUsage

	replayLiveBatchFlags.Usage = replayLiveBatchUsage
	replayLiveQueryFlags.Usage = replayLiveQueryUsage
}

// helper methods

type responseError struct {
	Err string `json:"Error"`
}

func (e responseError) Error() string {
	return e.Err
}

func connect(url string) (*client.Client, error) {
	return client.New(client.Config{
		URL: url,
	})
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
		case "define-template":
			defineTemplateFlags.Usage()
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
		case "show-template":
			showTemplateUsage()
		case "level":
			levelUsage()
		case "help":
			helpUsage()
		case "stats":
			statsUsage()
		case "version":
			versionUsage()
		case "vars":
			varsUsage()
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
	rsTask            = recordStreamFlags.String("task", "", "The ID of a task. Uses the dbrp value for the task.")
	rsDur             = recordStreamFlags.String("duration", "", "How long to record the data stream.")
	rsNowait          = recordStreamFlags.Bool("no-wait", false, "Do not wait for the recording to finish.")
	rsId              = recordStreamFlags.String("recording-id", "", "The ID to give to this recording. If not set an random ID is chosen.")

	recordBatchFlags = flag.NewFlagSet("record-batch", flag.ExitOnError)
	rbTask           = recordBatchFlags.String("task", "", "The ID of a task. Uses the queries contained in the task.")
	rbStart          = recordBatchFlags.String("start", "", "The start time for the set of queries.")
	rbStop           = recordBatchFlags.String("stop", "", "The stop time for the set of queries (default now).")
	rbPast           = recordBatchFlags.String("past", "", "Set start time via 'now - past'.")
	rbCluster        = recordBatchFlags.String("cluster", "", "Optional named InfluxDB cluster from configuration.")
	rbNowait         = recordBatchFlags.Bool("no-wait", false, "Do not wait for the recording to finish.")
	rbId             = recordBatchFlags.String("recording-id", "", "The ID to give to this recording. If not set an random ID is chosen.")

	recordQueryFlags = flag.NewFlagSet("record-query", flag.ExitOnError)
	rqQuery          = recordQueryFlags.String("query", "", "The query to record.")
	rqType           = recordQueryFlags.String("type", "", "The type of the recording to save (stream|batch).")
	rqCluster        = recordQueryFlags.String("cluster", "", "Optional named InfluxDB cluster from configuration.")
	rqNowait         = recordQueryFlags.Bool("no-wait", false, "Do not wait for the recording to finish.")
	rqId             = recordQueryFlags.String("recording-id", "", "The ID to give to this recording. If not set an random ID is chosen.")
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

	$ kapacitor record stream -task mem_free -duration 1m

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

	$ kapacitor record batch -task cpu_idle -start 2015-09-01T00:00:00Z -stop 2015-09-02T00:00:00Z

		This records the result of the query defined in task 'cpu_idle' and runs the query
		until the queries reaches the stop time, starting at time 'start' and incrementing
		by the schedule defined in the task.

	$ kapacitor record batch -task cpu_idle -past 10h

		This records the result of the query defined in task 'cpu_idle' and runs the query
		until the queries reaches the present time.
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
	var recording client.Recording
	var err error

	noWait := false

	switch args[0] {
	case "stream":
		recordStreamFlags.Parse(args[1:])
		if *rsTask == "" || *rsDur == "" {
			recordStreamFlags.Usage()
			return errors.New("both task and duration are required")
		}
		var duration time.Duration
		duration, err = influxql.ParseDuration(*rsDur)
		if err != nil {
			return err
		}
		noWait = *rsNowait
		recording, err = cli.RecordStream(client.RecordStreamOptions{
			ID:   *rsId,
			Task: *rsTask,
			Stop: time.Now().Add(duration),
		})
		if err != nil {
			return err
		}
	case "batch":
		recordBatchFlags.Parse(args[1:])
		if *rbTask == "" {
			recordBatchFlags.Usage()
			return errors.New("task is required")
		}
		if *rbStart == "" && *rbPast == "" {
			recordBatchFlags.Usage()
			return errors.New("must set one of start or past flags.")
		}
		if *rbStart != "" && *rbPast != "" {
			recordBatchFlags.Usage()
			return errors.New("cannot set both start and past flags.")
		}
		start, stop := time.Time{}, time.Now()
		if *rbStart != "" {
			start, err = time.Parse(time.RFC3339Nano, *rbStart)
			if err != nil {
				return err
			}
		}
		if *rbStop != "" {
			stop, err = time.Parse(time.RFC3339Nano, *rbStop)
			if err != nil {
				return err
			}
		}
		if *rbPast != "" {
			past, err := influxql.ParseDuration(*rbPast)
			if err != nil {
				return err
			}
			start = stop.Add(-1 * past)
		}
		noWait = *rbNowait
		recording, err = cli.RecordBatch(client.RecordBatchOptions{
			ID:      *rbId,
			Task:    *rbTask,
			Cluster: *rbCluster,
			Start:   start,
			Stop:    stop,
		})
		if err != nil {
			return err
		}
	case "query":
		recordQueryFlags.Parse(args[1:])
		if *rqQuery == "" || *rqType == "" {
			recordQueryFlags.Usage()
			return errors.New("both query and type are required")
		}
		var typ client.TaskType
		switch *rqType {
		case "stream":
			typ = client.StreamTask
		case "batch":
			typ = client.BatchTask
		}
		noWait = *rqNowait
		recording, err = cli.RecordQuery(client.RecordQueryOptions{
			ID:      *rqId,
			Query:   *rqQuery,
			Type:    typ,
			Cluster: *rqCluster,
		})
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("Unknown record type %q, expected 'stream', 'batch' or 'query'", args[0])
	}
	if noWait {
		fmt.Println(recording.ID)
		return nil
	}
	for recording.Status == client.Running {
		time.Sleep(500 * time.Millisecond)
		recording, err = cli.Recording(recording.Link)
		if err != nil {
			return err
		}
	}
	fmt.Println(recording.ID)
	if recording.Status == client.Failed {
		if recording.Error == "" {
			recording.Error = "recording failed: unknown reason"
		}
		return errors.New(recording.Error)
	}
	return nil
}

// Define
var (
	defineFlags = flag.NewFlagSet("define", flag.ExitOnError)
	dtick       = defineFlags.String("tick", "", "Path to the TICKscript")
	dtype       = defineFlags.String("type", "", "The task type (stream|batch)")
	dtemplate   = defineFlags.String("template-id", "", "Optional template ID")
	dvars       = defineFlags.String("vars", "", "Optional path to a JSON vars file")
	dnoReload   = defineFlags.Bool("no-reload", false, "Do not reload the task even if it is enabled")
	ddbrp       = make(dbrps, 0)
)

func init() {
	defineFlags.Var(&ddbrp, "dbrp", `A database and retention policy pair of the form "db"."rp" the quotes are optional. The flag can be specified multiple times.`)
}

type dbrps []client.DBRP

func (d *dbrps) String() string {
	return fmt.Sprint(*d)
}

// Parse string of the form "db"."rp" where the quotes are optional but can include escaped quotes
// within the strings.
func (d *dbrps) Set(value string) error {
	dbrp := client.DBRP{}
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

// read from txt starting with beginning quote until next unescaped quote.
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
	var u = `Usage: kapacitor define <task ID> [options]

	Create or update a task.

	A task is defined via a TICKscript that defines the data processing pipeline of the task.

	If an option is absent it will be left unmodified.

	If the task is enabled then it will be reloaded unless -no-reload is specified.

For example:

	You must define a task for the first time with all the flags.

		$ kapacitor define my_task -tick path/to/TICKscript -type stream -dbrp mydb.myrp

	Later you can change a single property of the task by referencing its name
	and only providing the single option you wish to modify.

		$ kapacitor define my_task -tick path/to/TICKscript

	or

		$ kapacitor define my_task -dbrp mydb.myrp -dbrp otherdb.default

	NOTE: you must specify all 'dbrp' flags you desire if you wish to modify them.

Options:

`
	fmt.Fprintln(os.Stderr, u)
	defineFlags.PrintDefaults()
}

func doDefine(args []string) error {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Must provide a task ID.")
		defineFlags.Usage()
		os.Exit(2)
	}
	defineFlags.Parse(args[1:])
	id := args[0]

	var script string
	if *dtick != "" {
		file, err := os.Open(*dtick)
		if err != nil {
			return err
		}
		defer file.Close()
		data, err := ioutil.ReadAll(file)
		if err != nil {
			return err
		}
		script = string(data)
	}

	var ttype client.TaskType
	switch *dtype {
	case "stream":
		ttype = client.StreamTask
	case "batch":
		ttype = client.BatchTask
	}

	vars := make(client.Vars)
	if *dvars != "" {
		f, err := os.Open(*dvars)
		if err != nil {
			return errors.Wrapf(err, "faild to open file %s", *dvars)
		}
		defer f.Close()
		dec := json.NewDecoder(f)
		if err := dec.Decode(&vars); err != nil {
			return errors.Wrapf(err, "invalid JSON in file %s", *dvars)
		}
	}

	l := cli.TaskLink(id)
	task, _ := cli.Task(l, nil)
	var err error
	if task.ID == "" {
		_, err = cli.CreateTask(client.CreateTaskOptions{
			ID:         id,
			TemplateID: *dtemplate,
			Type:       ttype,
			DBRPs:      ddbrp,
			TICKscript: script,
			Vars:       vars,
			Status:     client.Disabled,
		})
	} else {
		err = cli.UpdateTask(
			l,
			client.UpdateTaskOptions{
				TemplateID: *dtemplate,
				Type:       ttype,
				DBRPs:      ddbrp,
				TICKscript: script,
				Vars:       vars,
			},
		)
	}
	if err != nil {
		return err
	}

	if !*dnoReload && task.Status == client.Enabled {
		err := cli.UpdateTask(l, client.UpdateTaskOptions{Status: client.Disabled})
		if err != nil {
			return err
		}
		err = cli.UpdateTask(l, client.UpdateTaskOptions{Status: client.Enabled})
		if err != nil {
			return err
		}
	}
	return nil
}

// DefineTemplate
var (
	defineTemplateFlags = flag.NewFlagSet("define-template", flag.ExitOnError)
	dtTick              = defineTemplateFlags.String("tick", "", "Path to the TICKscript")
	dtType              = defineTemplateFlags.String("type", "", "The template type (stream|batch)")
)

func defineTemplateUsage() {
	var u = `Usage: kapacitor define-template <template ID> [options]

	Create or update a template.

	A template is defined via a TICKscript that defines the data processing pipeline of the template.

	If an option is absent it will be left unmodified.

	NOTE: Updating a template will reload all associated tasks.

For example:

	You must define a template for the first time with all the flags.

		$ kapacitor define-template my_template -tick path/to/TICKscript -type stream

	Later you can change a single property of the template by referencing its name
	and only providing the single option you wish to modify.

		$ kapacitor define-template my_template -tick path/to/TICKscript

	or

		$ kapacitor define-template my_template -type batch

Options:

`
	fmt.Fprintln(os.Stderr, u)
	defineTemplateFlags.PrintDefaults()
}

func doDefineTemplate(args []string) error {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Must provide a template ID.")
		defineTemplateFlags.Usage()
		os.Exit(2)
	}
	defineTemplateFlags.Parse(args[1:])
	id := args[0]

	var script string
	if *dtTick != "" {
		file, err := os.Open(*dtTick)
		if err != nil {
			return err
		}
		defer file.Close()
		data, err := ioutil.ReadAll(file)
		if err != nil {
			return err
		}
		script = string(data)
	}

	var ttype client.TaskType
	switch *dtType {
	case "stream":
		ttype = client.StreamTask
	case "batch":
		ttype = client.BatchTask
	}

	l := cli.TemplateLink(id)
	template, _ := cli.Template(l, nil)
	var err error
	if template.ID == "" {
		_, err = cli.CreateTemplate(client.CreateTemplateOptions{
			ID:         id,
			Type:       ttype,
			TICKscript: script,
		})
	} else {
		err = cli.UpdateTemplate(
			l,
			client.UpdateTemplateOptions{
				Type:       ttype,
				TICKscript: script,
			},
		)
	}
	return err
}

// Replay
var (
	replayFlags = flag.NewFlagSet("replay", flag.ExitOnError)
	rtask       = replayFlags.String("task", "", "The task ID.")
	rrecording  = replayFlags.String("recording", "", "The recording ID.")
	rreal       = replayFlags.Bool("real-clock", false, "If set, replay the data in real time. If not set replay data as fast as possible.")
	rrec        = replayFlags.Bool("rec-time", false, "If set, use the times saved in the recording instead of present times.")
	rnowait     = replayFlags.Bool("no-wait", false, "Do not wait for the replay to finish.")
	rid         = replayFlags.String("replay-id", "", "The ID to give to this replay. If not set a random ID is chosen.")
)

func replayUsage() {
	var u = `Usage: kapacitor replay [options]

Replay a recording to a task. Prints the replay ID.

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
	if *rrecording == "" {
		replayUsage()
		return errors.New("must pass recording ID")
	}
	if *rtask == "" {
		replayUsage()
		return errors.New("must pass task ID")
	}

	clk := client.Fast
	if *rreal {
		clk = client.Real
	}
	replay, err := cli.CreateReplay(client.CreateReplayOptions{
		ID:            *rid,
		Task:          *rtask,
		Recording:     *rrecording,
		RecordingTime: *rrec,
		Clock:         clk,
	})
	if err != nil {
		return err
	}
	if *rnowait {
		fmt.Println(replay.ID)
		return nil
	}
	for replay.Status == client.Running {
		time.Sleep(500 * time.Millisecond)
		replay, err = cli.Replay(replay.Link)
		if err != nil {
			return err
		}
	}
	fmt.Println(replay.ID)
	if replay.Status == client.Failed {
		if replay.Error == "" {
			replay.Error = "replay failed: unknown reason"
		}
		return errors.New(replay.Error)
	}
	return nil
}

// Replay Live
var (
	replayLiveBatchFlags = flag.NewFlagSet("replay-live-batch", flag.ExitOnError)
	rlbTask              = replayLiveBatchFlags.String("task", "", "The task ID.")
	rlbReal              = replayLiveBatchFlags.Bool("real-clock", false, "If set, replay the data in real time. If not set replay data as fast as possible.")
	rlbRec               = replayLiveBatchFlags.Bool("rec-time", false, "If set, use the times saved in the recording instead of present times.")
	rlbNowait            = replayLiveBatchFlags.Bool("no-wait", false, "Do not wait for the replay to finish.")
	rlbId                = replayLiveBatchFlags.String("replay-id", "", "The ID to give to this replay. If not set a random ID is chosen.")
	rlbStart             = replayLiveBatchFlags.String("start", "", "The start time for the set of queries.")
	rlbStop              = replayLiveBatchFlags.String("stop", "", "The stop time for the set of queries (default now).")
	rlbPast              = replayLiveBatchFlags.String("past", "", "Set start time via 'now - past'.")
	rlbCluster           = replayLiveBatchFlags.String("cluster", "", "Optional named InfluxDB cluster from configuration.")

	replayLiveQueryFlags = flag.NewFlagSet("replay-live-query", flag.ExitOnError)
	rlqTask              = replayLiveQueryFlags.String("task", "", "The task ID.")
	rlqReal              = replayLiveQueryFlags.Bool("real-clock", false, "If set, replay the data in real time. If not set replay data as fast as possible.")
	rlqRec               = replayLiveQueryFlags.Bool("rec-time", false, "If set, use the times saved in the recording instead of present times.")
	rlqNowait            = replayLiveQueryFlags.Bool("no-wait", false, "Do not wait for the replay to finish.")
	rlqId                = replayLiveQueryFlags.String("replay-id", "", "The ID to give to this replay. If not set a random ID is chosen.")
	rlqQuery             = replayLiveQueryFlags.String("query", "", "The query to replay.")
	rlqCluster           = replayLiveQueryFlags.String("cluster", "", "Optional named InfluxDB cluster from configuration.")
)

func replayLiveUsage() {
	var u = `Usage: kapacitor replay-live <batch|query> [options]

Replay data to a task directly without saving a recording.

The command is a hybrid of the 'kapacitor record batch|query' and 'kapacitor replay' commands.

See either 'kapacitor replay-live batch' or 'kapacitor replay-live query' for more details
Examples:

	$ kapacitor replay-live batch -task cpu_idle -start 2015-09-01T00:00:00Z -stop 2015-09-02T00:00:00Z

		This replays the result of the query defined in task 'cpu_idle' and runs the query
		until the queries reaches the stop time, starting at time 'start' and incrementing
		by the schedule defined in the task.

	$ kapacitor replay-liave batch -task cpu_idle -past 10h

		This replays the result of the query defined in task 'cpu_idle' and runs the query
		until the queries reaches the present time.
		The starting time for the queries is 'now - 10h' and increments by the schedule defined in the task.
`
	fmt.Fprintln(os.Stderr, u)
}

func replayLiveBatchUsage() {
	var u = `Usage: kapacitor replay-live batch [options]

Replay data from the queries in a batch task against the task.

This is similar to 'kapacitor record batch ...' but without saving a recording.

Examples:

	$ kapacitor replay-live query -task cpu_alert -rec-time -query 'select value from "telegraf"."default"."cpu_idle" where time > now() - 1h and time < now()'

		This replays the result of the query against the cpu_alert task.


Options:
`
	fmt.Fprintln(os.Stderr, u)
	replayLiveBatchFlags.PrintDefaults()
}

func replayLiveQueryUsage() {
	var u = `Usage: kapacitor replay-live query [options]

Replay the result of a querty against a task.

This is similar to 'kapacitor record query...' but without saving a recording.

Options:
`
	fmt.Fprintln(os.Stderr, u)
	replayLiveQueryFlags.PrintDefaults()
}

func doReplayLive(args []string) error {
	var replay client.Replay
	var err error

	noWait := false

	switch args[0] {
	case "batch":
		replayLiveBatchFlags.Parse(args[1:])
		if *rlbTask == "" {
			replayLiveBatchFlags.Usage()
			return errors.New("task is required")
		}
		if *rlbStart == "" && *rlbPast == "" {
			replayLiveBatchFlags.Usage()
			return errors.New("must set one of start or past flags.")
		}
		if *rlbStart != "" && *rlbPast != "" {
			replayLiveBatchFlags.Usage()
			return errors.New("cannot set both start and past flags.")
		}
		start, stop := time.Time{}, time.Now()
		if *rlbStart != "" {
			start, err = time.Parse(time.RFC3339Nano, *rlbStart)
			if err != nil {
				return err
			}
		}
		if *rlbStop != "" {
			stop, err = time.Parse(time.RFC3339Nano, *rlbStop)
			if err != nil {
				return err
			}
		}
		if *rlbPast != "" {
			past, err := influxql.ParseDuration(*rlbPast)
			if err != nil {
				return err
			}
			start = stop.Add(-1 * past)
		}
		noWait = *rlbNowait
		clk := client.Fast
		if *rlbReal {
			clk = client.Real
		}
		replay, err = cli.ReplayBatch(client.ReplayBatchOptions{
			ID:            *rlbId,
			Task:          *rlbTask,
			Start:         start,
			Stop:          stop,
			Cluster:       *rlbCluster,
			RecordingTime: *rlbRec,
			Clock:         clk,
		})
		if err != nil {
			return err
		}
	case "query":
		replayLiveQueryFlags.Parse(args[1:])
		if *rlqQuery == "" || *rlqTask == "" {
			recordQueryFlags.Usage()
			return errors.New("both query and task are required")
		}
		noWait = *rlqNowait
		clk := client.Fast
		if *rlqReal {
			clk = client.Real
		}
		replay, err = cli.ReplayQuery(client.ReplayQueryOptions{
			ID:            *rlqId,
			Task:          *rlqTask,
			Query:         *rlqQuery,
			Cluster:       *rlqCluster,
			RecordingTime: *rlqRec,
			Clock:         clk,
		})
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("Unknown replay-live type %q, expected 'batch' or 'query'", args[0])
	}
	if noWait {
		fmt.Println(replay.ID)
		return nil
	}
	for replay.Status == client.Running {
		time.Sleep(500 * time.Millisecond)
		replay, err = cli.Replay(replay.Link)
		if err != nil {
			return err
		}
	}
	fmt.Println(replay.ID)
	if replay.Status == client.Failed {
		if replay.Error == "" {
			replay.Error = "replay failed: unknown reason"
		}
		return errors.New(replay.Error)
	}
	return nil
}

// Enable
func enableUsage() {
	var u = `Usage: kapacitor enable [task ID...]

	Enable and start a task running from the live data.

For example:

	You can enable by specific task ID.

		$ kapacitor enable cpu_alert

	Or, you can enable by glob:

		$ kapacitor enable *_alert
`
	fmt.Fprintln(os.Stderr, u)
}

func doEnable(args []string) error {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Must pass at least one task ID")
		enableUsage()
		os.Exit(2)
	}

	limit := 100
	for _, pattern := range args {
		offset := 0
		for {
			tasks, err := cli.ListTasks(&client.ListTasksOptions{
				Pattern: pattern,
				Fields:  []string{"link"},
				Offset:  offset,
				Limit:   limit,
			})
			if err != nil {
				return errors.Wrap(err, "listing tasks")
			}
			for _, task := range tasks {
				err := cli.UpdateTask(
					task.Link,
					client.UpdateTaskOptions{Status: client.Enabled},
				)
				if err != nil {
					return errors.Wrapf(err, "enabling task %s", task.ID)
				}
			}
			if len(tasks) != limit {
				break
			}
			offset += limit
		}
	}
	return nil
}

// Disable

func disableUsage() {
	var u = `Usage: kapacitor disable [task ID...]

	Disable and stop a task running.

For example:

	You can disable by specific task ID.

		$ kapacitor disable cpu_alert

	Or, you can disable by glob:

		$ kapacitor disable *_alert
`
	fmt.Fprintln(os.Stderr, u)
}

func doDisable(args []string) error {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Must pass at least one task ID")
		disableUsage()
		os.Exit(2)
	}

	limit := 100
	for _, pattern := range args {
		offset := 0
		for {
			tasks, err := cli.ListTasks(&client.ListTasksOptions{
				Pattern: pattern,
				Fields:  []string{"link"},
				Offset:  offset,
				Limit:   limit,
			})
			if err != nil {
				return errors.Wrap(err, "listing tasks")
			}
			for _, task := range tasks {
				err := cli.UpdateTask(
					task.Link,
					client.UpdateTaskOptions{Status: client.Disabled},
				)
				if err != nil {
					return errors.Wrapf(err, "disabling task %s", task.ID)
				}
			}
			if len(tasks) != limit {
				break
			}
			offset += limit
		}
	}
	return nil
}

// Reload

func reloadUsage() {
	var u = `Usage: kapacitor reload [task ID...]

	Disable then enable a running task.

For example:

	You can reload by specific task ID.

		$ kapacitor reload cpu_alert

	Or, you can reload by glob:

		$ kapacitor reload *_alert
`
	fmt.Fprintln(os.Stderr, u)
}

func doReload(args []string) error {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Must pass at least one task ID")
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
	var u = `Usage: kapacitor show [task ID]

	Show details about a specific task.
`
	fmt.Fprintln(os.Stderr, u)
}

func doShow(args []string) error {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "Must specify one task ID")
		showUsage()
		os.Exit(2)
	}

	t, err := cli.Task(cli.TaskLink(args[0]), nil)
	if err != nil {
		return err
	}

	fmt.Println("ID:", t.ID)
	fmt.Println("Error:", t.Error)
	fmt.Println("Template:", t.TemplateID)
	fmt.Println("Type:", t.Type)
	fmt.Println("Status:", t.Status)
	fmt.Println("Executng:", t.Executing)
	fmt.Println("Created:", t.Created.Format(time.RFC822))
	fmt.Println("Modified:", t.Modified.Format(time.RFC822))
	fmt.Println("LastEnabled:", t.LastEnabled.Format(time.RFC822))
	fmt.Println("Databases Retenton Policies:", t.DBRPs)
	fmt.Printf("TICKscript:\n%s\n", t.TICKscript)
	if len(t.Vars) > 0 {
		fmt.Println("Vars:")
		varOutFmt := "%-30s%-10v%-40v\n"
		fmt.Printf(varOutFmt, "Name", "Type", "Value")
		vars := make([]string, 0, len(t.Vars))
		for name := range t.Vars {
			vars = append(vars, name)
		}
		sort.Strings(vars)
		for _, name := range vars {
			v := t.Vars[name]
			value := v.Value
			if list, ok := v.Value.([]client.Var); ok {
				var err error
				value, err = varListToStr(list)
				if err != nil {
					return errors.Wrapf(err, "invalid var %s", name)
				}
			}
			fmt.Printf(varOutFmt, name, v.Type, value)
		}
	}
	fmt.Printf("DOT:\n%s\n", t.Dot)

	return nil
}

func varListToStr(list []client.Var) (string, error) {
	values := make([]string, len(list))
	for i := range list {
		var str string
		switch list[i].Type {
		case client.VarString:
			s, ok := list[i].Value.(string)
			if !ok {
				return "", errors.New("non string value in string var")
			}
			str = s
		case client.VarStar:
			str = "*"
		default:
			return "", fmt.Errorf("lists can only contain strings or a star, got %s", list[i].Type)
		}
		values[i] = str
	}
	return "[" + strings.Join(values, ", ") + "]", nil
}

// Show Template

func showTemplateUsage() {
	var u = `Usage: kapacitor show-template [template ID]

	Show details about a specific template.
`
	fmt.Fprintln(os.Stderr, u)
}

func doShowTemplate(args []string) error {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "Must specify one template ID")
		showUsage()
		os.Exit(2)
	}

	t, err := cli.Template(cli.TemplateLink(args[0]), nil)
	if err != nil {
		return err
	}

	fmt.Println("ID:", t.ID)
	fmt.Println("Error:", t.Error)
	fmt.Println("Type:", t.Type)
	fmt.Println("Created:", t.Created.Format(time.RFC822))
	fmt.Println("Modified:", t.Modified.Format(time.RFC822))
	fmt.Printf("TICKscript:\n%s\n", t.TICKscript)
	fmt.Println("Vars:")
	varOutFmt := "%-30s%-10v%-40v%-40s\n"
	fmt.Printf(varOutFmt, "Name", "Type", "Default Value", "Description")
	vars := make([]string, 0, len(t.Vars))
	for name := range t.Vars {
		vars = append(vars, name)
	}
	sort.Strings(vars)
	for _, name := range vars {
		v := t.Vars[name]
		value := v.Value
		if v.Value == nil {
			value = "<required>"
		}
		if list, ok := v.Value.([]client.Var); ok {
			var err error
			value, err = varListToStr(list)
			if err != nil {
				return errors.Wrapf(err, "invalid var %s", name)
			}
		}
		fmt.Printf(varOutFmt, name, v.Type, value, v.Description)
	}
	fmt.Printf("DOT:\n%s\n", t.Dot)
	return nil
}

// List

func listUsage() {
	var u = `Usage: kapacitor list (tasks|templates|recordings|replays) [(task|template|recording|replay) ID or pattern]

List tasks, templates, recordings, or replays and their current state.

If no ID or pattern is given then all items will be listed.
`
	fmt.Fprintln(os.Stderr, u)
}

func doList(args []string) error {

	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "Must specify 'tasks', 'recordings', or 'replays'")
		listUsage()
		os.Exit(2)
	}

	if len(args) > 2 {
		fmt.Fprintln(os.Stderr, "Invalid usage of list")
		listUsage()
		os.Exit(2)
	}

	var pattern string
	if len(args) == 2 {
		pattern = args[1]
	}

	limit := 100

	switch kind := args[0]; kind {
	case "tasks":
		outFmt := "%-30s%-10v%-10v%-10v%s\n"
		fmt.Fprintf(os.Stdout, outFmt, "ID", "Type", "Status", "Executing", "Databases and Retention Policies")
		offset := 0
		for {
			tasks, err := cli.ListTasks(&client.ListTasksOptions{
				Pattern: pattern,
				Fields:  []string{"type", "status", "executing", "dbrps"},
				Offset:  offset,
				Limit:   limit,
			})
			if err != nil {
				return err
			}

			for _, t := range tasks {
				fmt.Fprintf(os.Stdout, outFmt, t.ID, t.Type, t.Status, t.Executing, t.DBRPs)
			}
			if len(tasks) != limit {
				break
			}
			offset += limit
		}
	case "templates":
		outFmt := "%-30s%-10v%-40v\n"
		fmt.Fprintf(os.Stdout, outFmt, "ID", "Type", "Vars")
		offset := 0
		for {
			templates, err := cli.ListTemplates(&client.ListTemplatesOptions{
				Pattern: pattern,
				Fields:  []string{"type", "vars"},
				Offset:  offset,
				Limit:   limit,
			})
			if err != nil {
				return err
			}

			for _, t := range templates {
				vars := make([]string, 0, len(t.Vars))
				for name := range t.Vars {
					vars = append(vars, name)
				}
				sort.Strings(vars)
				fmt.Fprintf(os.Stdout, outFmt, t.ID, t.Type, strings.Join(vars, ","))
			}
			if len(templates) != limit {
				break
			}
			offset += limit
		}
	case "recordings":
		outFmt := "%-40s%-8v%-10s%-10s%-23s\n"
		fmt.Fprintf(os.Stdout, outFmt, "ID", "Type", "Status", "Size", "Date")
		offset := 0
		for {
			recordings, err := cli.ListRecordings(&client.ListRecordingsOptions{
				Pattern: pattern,
				Fields:  []string{"type", "size", "date", "status"},
				Offset:  offset,
				Limit:   limit,
			})
			if err != nil {
				return err
			}

			for _, r := range recordings {
				fmt.Fprintf(os.Stdout, outFmt, r.ID, r.Type, r.Status, humanize.Bytes(uint64(r.Size)), r.Date.Local().Format(time.RFC822))
			}
			if len(recordings) != limit {
				break
			}
			offset += limit
		}
	case "replays":
		outFmt := "%-40v%-20v%-40v%-9v%-8v%-23v\n"
		fmt.Fprintf(os.Stdout, outFmt, "ID", "Task", "Recording", "Status", "Clock", "Date")
		offset := 0
		for {
			replays, err := cli.ListReplays(&client.ListReplaysOptions{
				Pattern: pattern,
				Fields:  []string{"task", "recording", "status", "clock", "date"},
				Offset:  offset,
				Limit:   limit,
			})
			if err != nil {
				return err
			}

			for _, r := range replays {
				fmt.Fprintf(os.Stdout, outFmt, r.ID, r.Task, r.Recording, r.Status, r.Clock, r.Date.Local().Format(time.RFC822))
			}
			if len(replays) != limit {
				break
			}
			offset += limit
		}
	default:
		return fmt.Errorf("cannot list '%s' did you mean 'tasks', 'recordings' or 'replays'?", kind)
	}
	return nil

}

// Delete
func deleteUsage() {
	var u = `Usage: kapacitor delete (tasks|templates|recordings|replays) [task|templates|recording|replay ID]...

	Delete a tasks, templates, recordings or replays.

	If a task is enabled it will be disabled and then deleted.

For example:

	You can delete task:

		$ kapacitor delete tasks my_task

	Or you can delete items by glob:

		$ kapacitor delete tasks *_alert

	You can delete recordings:

		$ kapacitor delete recordings b0a2ba8a-aeeb-45ec-bef9-1a2939963586
`
	fmt.Fprintln(os.Stderr, u)
}

func doDelete(args []string) error {
	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, "Must pass at least one ID")
		deleteUsage()
		os.Exit(2)
	}

	limit := 100
	switch kind := args[0]; kind {
	case "tasks":
		for _, pattern := range args[1:] {
			for {
				tasks, err := cli.ListTasks(&client.ListTasksOptions{
					Pattern: pattern,
					Fields:  []string{"link"},
					Limit:   limit,
				})
				if err != nil {
					return err
				}
				for _, task := range tasks {
					err := cli.DeleteTask(task.Link)
					if err != nil {
						return err
					}
				}
				if len(tasks) != limit {
					break
				}
			}
		}
	case "templates":
		for _, pattern := range args[1:] {
			for {
				templates, err := cli.ListTemplates(&client.ListTemplatesOptions{
					Pattern: pattern,
					Fields:  []string{"link"},
					Limit:   limit,
				})
				if err != nil {
					return err
				}
				for _, template := range templates {
					err := cli.DeleteTemplate(template.Link)
					if err != nil {
						return err
					}
				}
				if len(templates) != limit {
					break
				}
			}
		}
	case "recordings":
		for _, pattern := range args[1:] {
			for {
				recordings, err := cli.ListRecordings(&client.ListRecordingsOptions{
					Pattern: pattern,
					Fields:  []string{"link"},
					Limit:   limit,
				})
				if err != nil {
					return err
				}
				for _, recording := range recordings {
					err := cli.DeleteRecording(recording.Link)
					if err != nil {
						return err
					}
				}
				if len(recordings) != limit {
					break
				}
			}
		}
	case "replays":
		for _, pattern := range args[1:] {
			for {
				replays, err := cli.ListReplays(&client.ListReplaysOptions{
					Pattern: pattern,
					Fields:  []string{"link"},
					Limit:   limit,
				})
				if err != nil {
					return err
				}
				for _, replay := range replays {
					err := cli.DeleteReplay(replay.Link)
					if err != nil {
						return err
					}
				}
				if len(replays) != limit {
					break
				}
			}
		}
	default:
		return fmt.Errorf("cannot delete '%s' did you mean 'tasks', 'recordings' or 'replays'?", kind)
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
	return cli.LogLevel(args[0])
}

// Stats
func statsUsage() {
	var u = `Usage: kapacitor stats <general|ingress>

	Print stats about a certain aspect of Kapacitor.

	general - Display summary stats about Kapacitor.
	ingress - Display stats about the data Kapacitor is receiving by database, retention policy and measurement.
`
	fmt.Fprintln(os.Stderr, u)
}

func doStats(args []string) error {
	if len(args) != 1 {
		statsUsage()
		return errors.New("must provide a stats category")
	}
	vars, err := cli.DebugVars()
	if err != nil {
		return err
	}
	switch args[0] {
	case "general":
		outFmtNum := "%-30s%-30d\n"
		outFmtStr := "%-30s%-30s\n"
		fmt.Fprintf(os.Stdout, outFmtStr, "ClusterID:", vars.ClusterID)
		fmt.Fprintf(os.Stdout, outFmtStr, "ServerID:", vars.ServerID)
		fmt.Fprintf(os.Stdout, outFmtStr, "Host:", vars.Host)
		fmt.Fprintf(os.Stdout, outFmtNum, "Tasks:", vars.NumTasks)
		fmt.Fprintf(os.Stdout, outFmtNum, "Enabled Tasks:", vars.NumEnabledTasks)
		fmt.Fprintf(os.Stdout, outFmtNum, "Subscriptions:", vars.NumSubscriptions)
		fmt.Fprintf(os.Stdout, outFmtStr, "Version:", vars.Version)
	case "ingress":
		outFmt := "%-30s%-30s%-30s%-20.0f\n"
		fmt.Fprintf(os.Stdout, "%-30s%-30s%-30s%-20s\n", "Database", "Retention Policy", "Measurement", "Points Received")
		for _, stat := range vars.Stats {
			if stat.Name != "ingress" || stat.Tags["task_master"] != "main" {
				continue
			}
			var pr float64
			if i, ok := stat.Values["points_received"].(float64); ok {
				pr = i
			}
			fmt.Fprintf(
				os.Stdout,
				outFmt,
				stat.Tags["database"],
				stat.Tags["retention_policy"],
				stat.Tags["measurement"],
				pr,
			)
		}
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

// Vars
func varsUsage() {
	var u = `Usage: kapacitor vars

	Print debug vars in JSON format.
`
	fmt.Fprintln(os.Stderr, u)
}

func doVars(args []string) error {
	r, err := http.Get(cli.URL() + "/kapacitor/v1/debug/vars")
	if err != nil {
		return err
	}
	defer r.Body.Close()
	io.Copy(os.Stdout, r.Body)
	return nil
}
