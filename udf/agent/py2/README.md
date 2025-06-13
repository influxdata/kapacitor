## Python2 UDF support in Kapacitor

For reasons of security maintenance, support for Python2 UDF's is ended as of release 1.8.  This directory is added as a courtesy to users who either are unable to upgrade their UDFs to Python3 or have not found time yet to do so.

__WE STRONGLY ENCOURAGE USERS TO UPGRADE THEIR UDFs TO PYTHON3.__ 

Please note that this directory may be permanently removed in a future release.

### To use the contents of this directory. 

1. Install the contents and dependencies using `pip2`.  Note that in the pip2 package list, this package will appear as `kapacitor-udf` version `1.7`.

```
$ pip2 install udf/agent/py2
```

2. Reference _this_ directory _instead_ of the standard `udf/agent/py` directory in the UDF function section of `kapacitor.conf`

_example_

```
    [udf.functions.py2avg]
       prog = "/usr/bin/python2"
       args = ["-u", "./udf/agent/examples/py2/moving_avg/moving_avg.py"]
       timeout = "10s"
       [udf.functions.py2avg.env]
           PYTHONPATH = "./udf/agent/py2"
```

3. Restart `kapacitord`.