// +build windows

//Platform dependent constants for tests on windows
package server_test

const (
	ExecutableSuffix    = ".exe"
	PythonExecutable    = "python"
	LogFileExpectedMode = 0666
	AlertLogPath        = `c:\alert.log`
)
