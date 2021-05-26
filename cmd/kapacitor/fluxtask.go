package main

import (
	"crypto/tls"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/influxdata/influx-cli/v2/api"
	"github.com/influxdata/influx-cli/v2/clients"
	"github.com/influxdata/influx-cli/v2/clients/task"
	"github.com/influxdata/influx-cli/v2/pkg/stdio"
	kclient "github.com/influxdata/kapacitor/client/v1"
	"github.com/urfave/cli/v2"
)

func createFluxTaskApp(url string, skipSSL bool) *cli.App {
	return &cli.App{
		Name:                 "kapacitor flux",
		Usage:                "Kapacitor flux task management",
		UsageText:            "kapacitor flux [command]",
		EnableBashCompletion: true,
		Before:               withApi(url, skipSSL),
		Commands: []*cli.Command{
			{
				Name:  "task",
				Usage: "Task management commands",
				Subcommands: []*cli.Command{
					newTaskLogCmd(),
					newTaskRunCmd(),
					newTaskCreateCmd(),
					newTaskDeleteCmd(),
					newTaskFindCmd(),
					newTaskUpdateCmd(),
					newTaskRetryFailedCmd(),
				},
			},
		},
	}
}

const TaskMaxPageSize = 500

type urlPrefixer struct {
	prefix       string
	roundTripper http.RoundTripper
}

func (u urlPrefixer) RoundTrip(request *http.Request) (*http.Response, error) {
	request = request.Clone(request.Context())
	request.URL.Path = u.prefix + request.URL.Path
	return u.roundTripper.RoundTrip(request)
}

var _ http.RoundTripper = urlPrefixer{}

func withApi(u string, skipSsl bool) cli.BeforeFunc {
	return func(ctx *cli.Context) error {
		parsedUrl, err := url.Parse(u)
		if err != nil {
			return err
		}
		// copy of initialization for http.DefaultTransport, see https://pkg.go.dev/net/http#DefaultTransport
		clientTransport := &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		}
		// But set the InsecureSkipVerify flag properly
		clientTransport.TLSClientConfig = &tls.Config{InsecureSkipVerify: skipSsl}
		conf := api.NewConfiguration()
		conf.Host = parsedUrl.Host
		conf.Scheme = parsedUrl.Scheme
		conf.UserAgent = kclient.DefaultUserAgent
		// All the client code we share with influxdb assumes the /api/v2/tasks endpoint. Translate to
		// the /kapacitor/v1/api/v2/tasks endpoint
		conf.HTTPClient = &http.Client{Transport: urlPrefixer{
			prefix:       "/kapacitor/v1",
			roundTripper: clientTransport,
		}}
		apiClient := api.NewAPIClient(conf)
		ctx.App.Metadata["api"] = apiClient
		return nil
	}
}

func getAPI(ctx *cli.Context) *api.APIClient {
	i, ok := ctx.App.Metadata["api"].(*api.APIClient)
	if !ok {
		panic("missing APIClient with token")
	}
	return i
}

func withClient() cli.BeforeFunc {
	return func(ctx *cli.Context) error {
		apiClient := getAPI(ctx)
		client := task.Client{
			CLI: clients.CLI{
				StdIO:            stdio.TerminalStdio,
				HideTableHeaders: false,
				PrintAsJSON:      ctx.Bool("json"),
			},
			TasksApi:      apiClient.TasksApi,
			AllowEmptyOrg: true,
		}
		ctx.App.Metadata["client"] = client
		return nil
	}
}

func getClient(ctx *cli.Context) task.Client {
	i, ok := ctx.App.Metadata["client"].(task.Client)
	if !ok {
		panic("missing APIClient with token")
	}
	return i
}

func commonFlags() []cli.Flag {
	return []cli.Flag{&cli.BoolFlag{
		Name:  "json",
		Usage: "Output data as JSON",
	}}
}

func newTaskCreateCmd() *cli.Command {
	var params task.CreateParams
	return &cli.Command{
		Name:      "create",
		Usage:     "Create a task with a Flux script provided via the first argument or a file or stdin",
		ArgsUsage: "[flux script or '-' for stdin]",
		Before:    withClient(),
		Flags: append(commonFlags(), &cli.StringFlag{
			Name:      "file",
			Usage:     "Path to Flux script file",
			Aliases:   []string{"f"},
			TakesFile: true,
		}),
		Action: func(ctx *cli.Context) error {
			client := getClient(ctx)
			var err error
			params.FluxQuery, err = clients.ReadQuery(ctx)
			if err != nil {
				return err
			}
			return client.Create(ctx.Context, &params)
		},
	}
}

func newTaskFindCmd() *cli.Command {
	var params task.FindParams
	flags := append(commonFlags(), []cli.Flag{
		&cli.StringFlag{
			Name:        "id",
			Usage:       "task ID",
			Aliases:     []string{"i"},
			Destination: &params.TaskID,
		},
		&cli.StringFlag{
			Name:        "user-id",
			Usage:       "task owner ID",
			Aliases:     []string{"n"},
			Destination: &params.UserID,
		},
		&cli.IntFlag{
			Name:        "limit",
			Usage:       "the number of tasks to find",
			Destination: &params.Limit,
			Value:       TaskMaxPageSize,
		},
	}...)
	return &cli.Command{
		Name:    "list",
		Usage:   "List tasks",
		Aliases: []string{"find", "ls"},
		Flags:   flags,
		Before:  withClient(),
		Action: func(ctx *cli.Context) error {
			client := getClient(ctx)
			return client.Find(ctx.Context, &params)
		},
	}
}

func newTaskRetryFailedCmd() *cli.Command {
	var params task.RetryFailedParams
	flags := append(commonFlags(), []cli.Flag{
		&cli.StringFlag{
			Name:        "id",
			Usage:       "task ID",
			Aliases:     []string{"i"},
			Destination: &params.TaskID,
		},
		&cli.StringFlag{
			Name:        "before",
			Usage:       "runs before this time",
			Destination: &params.RunFilter.Before,
		},
		&cli.StringFlag{
			Name:        "after",
			Usage:       "runs after this time",
			Destination: &params.RunFilter.After,
		},
		&cli.BoolFlag{
			Name:        "dry-run",
			Usage:       "print info about runs that would be retried",
			Destination: &params.DryRun,
		},
		&cli.IntFlag{
			Name:        "task-limit",
			Usage:       "max number of tasks to retry failed runs for",
			Destination: &params.TaskLimit,
			Value:       100,
		},
		&cli.IntFlag{
			Name:        "run-limit",
			Usage:       "max number of failed runs to retry per task",
			Destination: &params.RunFilter.Limit,
			Value:       100,
		},
	}...)
	return &cli.Command{
		Name:    "retry-failed",
		Usage:   "Retry failed runs",
		Aliases: []string{"rtf"},
		Flags:   flags,
		Before:  withClient(),
		Action: func(ctx *cli.Context) error {
			client := getClient(ctx)
			return client.RetryFailed(ctx.Context, &params)
		},
	}
}

func newTaskUpdateCmd() *cli.Command {
	var params task.UpdateParams
	flags := commonFlags()
	flags = append(flags, []cli.Flag{
		&cli.StringFlag{
			Name:        "id",
			Usage:       "task ID (required)",
			Aliases:     []string{"i"},
			Destination: &params.TaskID,
			Required:    true,
		},
		&cli.StringFlag{
			Name:        "status",
			Usage:       "update tasks status",
			Destination: &params.Status,
		},
		&cli.StringFlag{
			Name:      "file",
			Usage:     "Path to Flux script file",
			Aliases:   []string{"f"},
			TakesFile: true,
		},
	}...)
	return &cli.Command{
		Name:      "update",
		Usage:     "Update task status or script. Provide a Flux script via the first argument or a file.",
		ArgsUsage: "[flux script or '-' for stdin]",
		Flags:     flags,
		Before:    withClient(),
		Action: func(ctx *cli.Context) error {
			client := getClient(ctx)
			var err error
			if ctx.String("file") != "" || ctx.NArg() != 0 {
				params.FluxQuery, err = clients.ReadQuery(ctx)
				if err != nil {
					return err
				}
			}
			return client.Update(ctx.Context, &params)
		},
	}
}

func newTaskDeleteCmd() *cli.Command {
	var params task.DeleteParams
	flags := commonFlags()
	flags = append(flags, []cli.Flag{
		&cli.StringFlag{
			Name:        "id",
			Usage:       "task ID (required)",
			Aliases:     []string{"i"},
			Destination: &params.TaskID,
			Required:    true,
		},
	}...)
	return &cli.Command{
		Name:   "delete",
		Usage:  "Delete tasks",
		Flags:  flags,
		Before: withClient(),
		Action: func(ctx *cli.Context) error {
			client := getClient(ctx)
			return client.Delete(ctx.Context, &params)
		},
	}
}

func newTaskLogCmd() *cli.Command {
	return &cli.Command{
		Name:  "log",
		Usage: "Log related commands",
		Subcommands: []*cli.Command{
			newTaskLogFindCmd(),
		},
	}
}

func newTaskLogFindCmd() *cli.Command {
	var params task.LogFindParams
	flags := commonFlags()
	flags = append(flags, []cli.Flag{
		&cli.StringFlag{
			Name:        "task-id",
			Usage:       "task id (required)",
			Destination: &params.TaskID,
			Required:    true,
		},
		&cli.StringFlag{
			Name:        "run-id",
			Usage:       "run id",
			Destination: &params.RunID,
		},
	}...)
	return &cli.Command{
		Name:    "list",
		Usage:   "List logs for a task",
		Aliases: []string{"find", "ls"},
		Flags:   flags,
		Before:  withClient(),
		Action: func(ctx *cli.Context) error {
			client := getClient(ctx)
			return client.FindLogs(ctx.Context, &params)
		},
	}
}

func newTaskRunCmd() *cli.Command {
	return &cli.Command{
		Name:  "run",
		Usage: "Run related commands",
		Subcommands: []*cli.Command{
			newTaskRunFindCmd(),
			newTaskRunRetryCmd(),
		},
	}
}

func newTaskRunFindCmd() *cli.Command {
	var params task.RunFindParams
	flags := commonFlags()
	flags = append(flags, []cli.Flag{
		&cli.StringFlag{
			Name:        "task-id",
			Usage:       "task ID (required)",
			Destination: &params.TaskID,
			Required:    true,
		},
		&cli.StringFlag{
			Name:        "run-id",
			Usage:       "run id",
			Destination: &params.RunID,
		},
		&cli.StringFlag{
			Name:        "before",
			Usage:       "runs before this time",
			Destination: &params.Filter.Before,
		},
		&cli.StringFlag{
			Name:        "after",
			Usage:       "runs after this time",
			Destination: &params.Filter.After,
		},
		&cli.IntFlag{
			Name:        "limit",
			Usage:       "limit the results",
			Destination: &params.Filter.Limit,
			Value:       100,
		},
	}...)
	return &cli.Command{
		Name:    "list",
		Usage:   "List runs for a tasks",
		Aliases: []string{"find", "ls"},
		Flags:   flags,
		Before:  withClient(),
		Action: func(ctx *cli.Context) error {
			client := getClient(ctx)
			return client.FindRuns(ctx.Context, &params)
		},
	}
}

func newTaskRunRetryCmd() *cli.Command {
	var params task.RunRetryParams
	flags := commonFlags()
	flags = append(flags, []cli.Flag{
		&cli.StringFlag{
			Name:        "task-id",
			Usage:       "task ID (required)",
			Aliases:     []string{"i"},
			Destination: &params.TaskID,
			Required:    true,
		},
		&cli.StringFlag{
			Name:        "run-id",
			Usage:       "run ID (required)",
			Aliases:     []string{"r"},
			Destination: &params.RunID,
			Required:    true,
		},
	}...)
	return &cli.Command{
		Name:   "retry",
		Usage:  "Retry a run",
		Flags:  flags,
		Before: withClient(),
		Action: func(ctx *cli.Context) error {
			client := getClient(ctx)
			return client.RetryRun(ctx.Context, &params)
		},
	}
}
