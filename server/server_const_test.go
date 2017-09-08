// +build !windows

//Platform dependent constants for non-windows
package server_test

const (
	ExecutableSuffix    = ""
	PythonExecutable    = "python2"
	LogFileExpectedMode = 0604
	AlertLogPath        = `/var/log/alert.log`
)
