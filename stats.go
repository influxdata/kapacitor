package kapacitor

import (
	"expvar"
	"strconv"
)

const (
	// List of names for top-level exported vars
	ClusterIDVarName = "cluster_id"
	ServerIDVarName  = "server_id"
	HostVarName      = "host"
	ProductVarName   = "product"
	VersionVarName   = "version"

	NumTasksVarName         = "num_tasks"
	NumEnabledTasksVarName  = "num_enabled_tasks"
	NumSubscriptionsVarName = "num_subscriptions"

	// The name of the product
	Product = "kapacitor"
)

var (
	// Global expvars
	NumTasks         = &expvar.Int{}
	NumEnabledTasks  = &expvar.Int{}
	NumSubscriptions = &expvar.Int{}
)

func init() {
	expvar.Publish(NumTasksVarName, NumTasks)
	expvar.Publish(NumEnabledTasksVarName, NumEnabledTasks)
	expvar.Publish(NumSubscriptionsVarName, NumSubscriptions)
}

// Gets an exported var and returns its unquoted string contents
func GetStringVar(name string) string {
	s, err := strconv.Unquote(expvar.Get(name).String())
	if err != nil {
		panic(err)
	}
	return s
}

// Gets an exported var and returns its int value
func GetIntVar(name string) int64 {
	i, err := strconv.ParseInt(expvar.Get(name).String(), 10, 64)
	if err != nil {
		panic(err)
	}
	return i
}

// Gets an exported var and returns its float value
func GetFloatVar(name string) float64 {
	f, err := strconv.ParseFloat(expvar.Get(name).String(), 64)
	if err != nil {
		panic(err)
	}
	return f
}
