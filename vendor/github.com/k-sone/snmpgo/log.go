package snmpgo

type StdLogger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
}
