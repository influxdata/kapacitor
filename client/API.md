# Kapacitor API Reference Documentation

Kapacitor provides an HTTP API on port 9092 by default.
With the API you can control which tasks are executing, query status of tasks and manage recordings etc.

Each section below defines the available API endpoints and there inputs and outputs.

### Response Codes

All requests can return these response codes:

| HTTP Response Code | Meaning                                                                                                                                                             |
| ------------------ | -------                                                                                                                                                             |
| 200                | The request was a success the content is dependent on the request.                                                                                                  |
| 204                | The request was a success and no content was returned.                                                                                                              |
| 4xx                | Invalid request, refer to error for what it wrong with the request. Repeating the request will continue to return the same error.                                   |
| 5xx                | The server was unable to process the request, refer to the error for a reason. Repeating the request may result in a success if the server issue has been resolved. |

### Errors

All requests can return JSON in the following format to provide more information about a failed request.

```
{
    "error" : "error message"
}
```

### Query Parameters vs JSON body

To make using this API a consistent and easy experience we follow one simple rule for when extra information
about a request is found in the query parameters of the URL or when they are part of the submitted JSON body.

Query parameters are used only for GET requests and all POST requests expect parameters to be specified in the JSON body.

## Tasks

A task represents work for Kapacitor to perform.
A task is defined by its name, type, TICKscript, and list of database retention policy pairs it is allowed to access.

### Define Task

To define a task POST to the `/task/TASK_NAME` endpoint.
If no task exists it with that name it will be created.
Otherwise the existing task will be updated with the provided information.

Define a task using a JSON object with the following options:

| Parameter | Purpose                                                                |
| --------- | -------                                                                |
| type      | The task type: `stream` or `batch`.                                    |
| dbrps     | List of database retention policy pairs the task is allowed to access. |
| script    | The content of the script.                                             |

If any option is missing it will be left unmodified if the task already exists.

#### Example

```
POST /task/TASK_NAME
{
    "type" : "stream",
    "dbrps": [{"db": "DATABASE_NAME", "rp" : "RP_NAME"}],
    "script": "stream\n    |from()\n        .measurement('cpu')\n"
}
```

Modify only the dbrps of the task.

```
POST /task/TASK_NAME
{
    "dbrps": [{"db": "NEW_DATABASE_NAME", "rp" : "NEW_RP_NAME"}]
}
```

>NOTE: Setting any DBRP will overwrite all stored DBRPs.

#### Response

| Code | Meaning |
| ---- | ------- |
| 204  | Success |


### Get Task

To get information about a task make a GET request to the `/task/TASK_NAME` endpoint.

| Query Parameter | Default | Purpose                                                                                                                                                            |
| --------------- | ------- | -------                                                                                                                                                            |
| dot-labels      | false   | Return the DOT formatted string using only label attributes. This is useful if you intend to render a diagram from the DOT content. Otherwise it is less readable. |
| skip-format     | false   | Do not format the TICKscript before returning.                                                                                                                     |


#### Example

Get information about a task using defaults.

```
GET /task/TASK_NAME
```

```
{
    "name" : "TASK_NAME",
    "type" : "stream",
    "dbrps": [{"db": "DATABASE_NAME", "rp" : "RP_NAME"}],
    "script": "stream\n    |from()\n        .measurement('cpu')\n",
    "dot" : "digraph TASK_NAME { ... }",
    "enabled" : true,
    "executing" : true,
    "error" : "",
    "stats": {}
}
```

Get information about a task using only labels in the DOT content and skip the format step.

```
GET /task/TASK_NAME?skip-format=true&dot-labels=true
```

```
{
    "name" : "TASK_NAME",
    "type" : "stream",
    "dbrps": [{"db": "DATABASE_NAME", "rp" : "RP_NAME"}],
    "script": "stream|from().measurement('cpu')",
    "dot" : "digraph TASK_NAME { ... }",
    "enabled" : true,
    "executing" : true,
    "error" : "",
    "stats": {}
}
```

#### Response

| Code | Meaning             |
| ---- | -------             |
| 200  | Success             |
| 404  | Task does not exist |


### Enable Task

To enable a task and start it executing make a POST request to the `/task/TASK_NAME/enable` endpoint.


#### Example

```
POST /task/TASK_NAME/enable
```

#### Response

| Code | Meaning             |
| ---- | -------             |
| 204  | Success             |
| 404  | Task does not exist |

### Disable Task

To disable a task and stop it executing make a POST request to the `/task/TASK_NAME/disable` endpoint.

#### Example

```
POST /task/TASK_NAME/disable
```

#### Response

| Code | Meaning             |
| ---- | -------             |
| 204  | Success             |
| 404  | Task does not exist |


### Delete Task

To delete a task make a DELETE request to the `/task/TASK_NAME` endpoint.

```
DELETE /task/TASK_NAME
```

#### Response

| Code | Meaning             |
| ---- | -------             |
| 204  | Success             |

>NOTE: Deleting a non-existent task is not an error and will return a 204 success.


### List Tasks

To get information about several tasks make a GET request to the `/task` endpoint.

| Query Parameter | Purpose                                                                                                                                           |
| --------------- | -------                                                                                                                                           |
| pattern         | Filter results based on the pattern. Uses standard shell glob matching, see [this](https://golang.org/pkg/path/filepath/#Match) for more details. |

#### Example

Get all tasks.

```
GET /task
```

```
{
    "tasks" : [
        {
            "name" : "TASK_NAME",
            "type" : "stream",
            "dbrps": [{"db": "DATABASE_NAME", "rp" : "RP_NAME"}],
            "enabled" : true,
            "executing" : true,
            "stats": {}
        },
        {
            "name" : "ANOTHER_TASK_NAME",
            "type" : "batch",
            "dbrps": [{"db": "DATABASE_NAME", "rp" : "RP_NAME"}],
            "enabled" : true,
            "executing" : true,
            "stats": {}
        }
    ]
}
```

Optionally specify a glob `pattern` to list only matching tasks.

```
GET /task?pattern=TASK*
```

```
{
    "tasks" : [
        {
            "name" : "TASK_NAME",
            "type" : "stream",
            "dbrps": [{"db": "DATABASE_NAME", "rp" : "RP_NAME"}],
            "enabled" : true,
            "executing" : true,
            "stats": {}
        }
    ]
}
```

#### Response

| Code | Meaning                |
| ---- | -------                |
| 200  | Success                |
| 404  | No tasks match pattern |

## Replays and Recordings

Kapacitor can save recordings of data and replay them against a specified task.

### Start Recording

To create a recording make a POST request to the `/recording` endpoint.
There are three methods for recording data with Kapacitor:

| Method | Description                                        |
| ------ | -----------                                        |
| stream | Record the incoming stream of data.                |
| batch  | Record the results of the queries in a batch task. |
| query  | Record the result of an explicit query.            |

All record POST requests must specify the parameter `method` with one of the above options.
Each different `method` has it's own parameters.

The request returns once the recording is started and does not wait for it to finish.
A recording ID is returned to later identify the recording.

##### Stream

| Parameter | Purpose                                                             |
| --------- | -------                                                             |
| name      | Name of a task, used to only record data for the DBRPs of the task. |
| duration  | Duration string indicating how long to record the stream.           |

##### Batch

| Parameter | Purpose                                                                                                          |
| --------- | -------                                                                                                          |
| name      | Name of a task, records the results of the queries defined in the task.                                          |
| start     | Earliest date for which data will be recorded. RFC3339Nano formatted.                                            |
| stop      | Latest date for which data will be recorded. If not specified uses the current time. RFC3339Nano formatted data. |
| past      | Duration string indicating how far into the past to start recording data relative to the stop date.              |
| cluster   | Name of a configured InfluxDB cluster. If empty uses the default cluster.                                    |

Only one of `start` or `past` may be specified.

##### Query

| Parameter | Purpose                                                                       |
| --------- | -------                                                                       |
| type      | Type of recording, `stream` or `batch`.                                   |
| query     | Query to execute.                                                         |
| cluster   | Name of a configured InfluxDB cluster. If empty uses the default cluster. |

>NOTE: A recording itself is typed as either a stream or batch recording and can only be replayed to a task of a corresponding type.
Therefore when you record the result of a raw query you must specify the type recording you wish to create.


#### Example

Create a recording using the `stream` method

```
POST /recording
{
    "method" : "stream",
    "name" : "TASK_NAME",
    "duration" : "10m"
}
```


Create a recording using the `batch` method specifying a start time.

```
POST /recording
{
    "method" : "batch",
    "name" : "TASK_NAME",
    "start" : "2006-01-02T15:04:05Z07:00"
}
```

Create a recording using the `batch` method specifying a past duration.

```
POST /recording
{
    "method" : "batch",
    "name" : "TASK_NAME",
    "past" : "7d"
}
```

Create a recording using the `query` method specifying a `stream` type.

```
POST /recording
{
    "method" : "query",
    "query" : "SELECT mean(usage_idle) FROM cpu WHERE time > now() - 1h GROUP BY time(10m)",
    "type" : "stream"
}
```

Create a recording using the `query` method specifying a `batch` type.

```
POST /recording
{
    "method" : "query",
    "query" : "SELECT mean(usage_idle) FROM cpu WHERE time > now() - 1h GROUP BY time(10m)",
    "type" : "batch"
}
```


#### Response

All recordings are assigned an ID which is returned in this format.

```
{
    "id" : "e24db07d-1646-4bb3-a445-828f5049bea0"
}
```

| Code | Meaning                             |
| ---- | -------                             |
| 200  | Success, the recording has started. |

### Wait for Recording

In order to determine when a recording has finished you must request it by making a GET request to `/recording/RECORDING_ID`.
A GET request for a recording will block until the recording is complete.

As a result it is typical to make a POST request to start a recording and then immediately GET it in order to report when it has finished.

#### Example

```
GET /recording/e24db07d-1646-4bb3-a445-828f5049bea0
```

```
{
    "id" : "e24db07d-1646-4bb3-a445-828f5049bea0"
}
```

#### Response

| Code | Meaning                                   |
| ---- | -------                                   |
| 200  | Success if the recording was found.       |
| 404  | No recording exists for the specified ID. |

### Delete Recording

To delete a recording make a DELETE request to the `/recording/RECORDING_ID` endpoint.

```
DELETE /recording/RECORDING_ID
```

#### Response

| Code | Meaning             |
| ---- | -------             |
| 204  | Success             |

>NOTE: Deleting a non-existent recording is not an error and will return a 204 success.

### List Recordings

To list all recordings make a GET request to the `/recording` endpoint.

#### Example

```
GET /recording
```

```
{
    "recordings" : [
        {
            "id" : "e24db07d-1646-4bb3-a445-828f5049bea0",
            "type" : "stream",
            "size" : 1980353,
            "created" : "2006-01-02T15:04:05Z07:00",
            "error" : ""
        },
        {
            "id" : "8a4c06c6-30fb-42f4-ac4a-808aa31278f6",
            "type" : "batch",
            "size" : 216819562,
            "created" : "2006-01-02T15:04:05Z07:00",
            "error" : ""
        }
    ]
}
```

#### Response

| Code | Meaning |
| ---- | ------- |
| 200  | Success |


### Replay recording

To replay a recording make a POST request to `/recording/RECORDING_ID/replay`.
When replaying a recording specify these options:

| Parameter      | Default | Purpose                                                                                                                                                                                                                                       |
| ----------     | ------- | -------                                                                                                                                                                                                                                       |
| name           |         | The recording will be replayed against this task.                                                                                                                                                                                             |
| recording-time | false   | If true, use the times in the recording, otherwise adjust times relative to the current time.                                                                                                                                                 |
| clock          | fast    | One of `fast`, `real`. If `real` wait for real time to pass corresponding with the time in the recordings. If `fast` replay data without delay. For example, if fast is `real` then a stream recording of duration 5m will take 5m to replay. |

#### Example

Replay a recording using default parameters.

```
POST /recording/RECORDING_ID/replay
{
    "name" : "TASK_NAME",
}
```

Replay a recording in real-time mode and preserve recording times.

```
POST /recording/RECORDING_ID/replay
{
    "name" : "TASK_NAME",
    "clock" : "real",
    "recording-time" : true,
}
```

#### Response

The request blocks until the replay is complete.

| Code | Meaning                                                   |
| ---- | -------                                                   |
| 204  | Success                                                   |
| 404  | No recording or task exists for the specified ID or name. |


## Miscellaneous

### Ping

You can 'ping' the Kapacitor server to validate you have a successful connection.
A ping request does nothing but respond with a 204.

>NOTE: The Kapacitor server version is returned in the `X-Kapacitor-Version` HTTP header on all requests.
Ping is a useful request if you simply need the verify the version of server you are talking to.

#### Example

```
GET /ping
```

#### Response

| Code | Meaning |
| ---- | ------- |
| 204  | Success |


