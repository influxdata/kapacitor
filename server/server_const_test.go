// +build !windows

//Platform dependent constants for non-windows
package server_test

const (
	ExecutableSuffix    = ""
	Python2Executable   = "python2"
	PythonExecutable    = "python"
	LogFileExpectedMode = 0604
	AlertLogPath        = `/var/log/alert.log`
)
