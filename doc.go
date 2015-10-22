/*
	A data pipeline processing engine.

	See the README for more complete examples and guides.

	Code Organization:

	The pipeline package provides an API for how nodes can be connected to form a pipeline.
	The individual implementations of each node exist in this kapacitor package.
	The reason for the separation is to keep the exported API from the pipeline package
	clean as it is consumed via the TICKscripts (a DSL for Kapacitor).

	Other Concepts:

	Stream vs Batch -- Use of the word 'stream'  indicates data arrives a single data point at a time.
	Use of the word 'batch' indicates data arrives in sets or batches or data points.

	Task -- A task represents a concrete workload to perform.
	It consists of a pipeline and an identifying name.
	Basic CRUD operations can be performed on tasks.

	Task Master -- Responsible for executing a task in a specific environment.

	Replay -- Replays static datasets against tasks.
*/
package kapacitor
