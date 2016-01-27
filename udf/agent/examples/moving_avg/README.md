# Moving Average UDF Examples

Each example in this directory should implement a moving average with the following specifications.

* Wants and provides a stream edge.
* Computes a moving average and emits a point for every incoming points once the window is full.
* Has a `field` option that specifies which field to operate on.
* Has a `size` option that specifies how many points to keep in the moving average window.
* Has an `as` option that specifies the name of the output field.
* Implements snapshot/restore methods that save the state of the moving average.


