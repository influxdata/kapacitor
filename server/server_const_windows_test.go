// +build windows

//Platform dependent constants for tests on windows
package server_test

const (
	ExecutableSuffix = ".exe"
	// For windows we won't test python2 explicitly
	Python2Executable   = "python"
	PythonExecutable    = "python"
	LogFileExpectedMode = 0666
	AlertLogPath        = `c:\alert.log`
)
