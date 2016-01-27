# Outlier UDF Examples

Each example in this directory should implement a simple outlier detection algorithm.

The algorithm:

Find outliers via the Tukey method.
Return all outliers that are more outside lower and upper bounds.

The bounds are defined by:

Lower Bound = 1st Quartile - IQR*SCALE
Upper Bound = 3rd Quartile + IQR*SCALE

where

IQR = 3rd Quartile - 1st Quartile
SCALE = a user defined value >= 1.0

To implement this method one must first find
the 1st and 3rd quartiles and then compute the
lower and upper bounds.

The quartiles are to be calculated via the median method.
First compute the median of the entire data set.
Then compute the median of each half of the data set with neither half containing the median.
The medians of each half are the first and third quartiles.
To compute the median with even number of points return the arithmetic mean of the two middle points.


The UDF should implement the above algorithm and follow these specifications.


* Wants and provides a batch edge.
* For each batch received return a batch with only the outlier points included unmodified.
* Has a `field` option that specifies which field to operate on.
* Has a `scale` option that specifies a scale multiplier to the IQR value. Default is 1.5.

