# UDF Agents and Servers

A UDF is a User Defined Function, meaning that you can write your own functions/algorithms and plug them into Kapacitor.
Your custom function runs in its own process and Kapacitor communicates with it via a defined protocol, see [udf.proto](https://github.com/influxdata/kapacitor/blob/master/udf/udf.proto).
To facilitate working with the protocol several `agents` have been written in various languages that abstract the protocol communication through an interface in the respective languages.
You can find those agent implementations in this directory and subdirectories based on language name.


Example uses of the agents can be found in the `examples` directory.
These examples are working examples and are executed as part of the testing suite,
see [server_test.go](https://github.com/influxdata/kapacitor/blob/master/cmd/kapacitord/run/server_test.go).

## Child process vs Socket

There are two approaches for writing UDFs.

* A child process based approach where Kapacitor spawns a child process and communicates over STDIN/STDOUT.
* A socket based approach where you start the UDF process externally and Kapacitor connects to it over a socket.

For the socket based approach there will only ever be one instance of your UDF process running.
Each use of the UDF in a TICKscript will be a new connection the socket.
Where as each use of a process based UDF means a new child process is spawned for each.

## Design

The protocol for communicating with Kapacitor consists of Request and Response messages.
The agents wrap the communication and serialization and expose an interface that needs to be implemented to handle each request/response.
In addition to the request/response paradigm agents provide a way to stream data back to Kapacitor.
Your UDF is in control of when new points or batches are sent back to Kapacitor.


### Agents and Servers

There are two main objects provided in the current implementations, an `Agent` and a `Server`.
The `Agent` is responsible for managing the communication over input and output streams.
The `Server` is responsible for accepting new connections and creating new `Agents` to handle those new connections.

Both process based and socket based UDFs will need to use an `Agent` to handle the communication/serialization aspects of the protocol.
Only socket based UDFs need use the `Server`.

## Writing an Agent for a new Language

The UDF protocol is designed to be simple and consists of reading and writing protocol buffer messages.

In order to write a UDF in the language of your choice your language must have protocol buffer support and be able to read and write to a socket.

The basic steps are:

0. Add the language to the `udf/io.go` generate comment so the udf.proto code exists for your language.
1. Implement a Varint encoder/decoder, this is trivial see the python implementation.
2. Implement a method for reading and writing streamed protobuf messages. See `udf.proto` for more details.
3. Create an interface for handling each of the request/responses.
4. Write a loop for reading from an input stream and calling the handler interface, and write responses to an output stream.
5. Provide an thread safe mechanism for writing points and batches to the output stream independent of the handler interface.
    This is easily accomplished with a synchronized write method, see the python implementation.
6. Implement the examples using your new agent.
7. Add your example to the test suite in `cmd/kapacitord/run/server_test.go`.

For process based UDFs it is expected that the process terminate after STDIN is closed and the remaining requests processed.
After STDIN is closed, the agent process can continue to send Responses to Kapacitor as long as a keepalive timeout does not occur.
Once a keepalive timeout is reached and after a 2*keepalive_time grace period, if the process has not terminated then it will be forcefully terminated.

## Docker

It is expected that the example can run inside the test suite.
Since generating different protocol buffer code requires different plugins and libraries to run we make use of Docker to provide the necessary environment.
This makes testing the code easier as the developer does not have to install each supported language locally.

