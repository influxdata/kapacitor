package vars

import (
	"expvar"
	"time"

	kexpvar "github.com/influxdata/kapacitor/expvar"
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

	UptimeVarName = "uptime"

	// The name of the product
	Product = "kapacitor"
)

var (
	// Global expvars
	NumTasksVar         = &kexpvar.Int{}
	NumEnabledTasksVar  = &kexpvar.Int{}
	NumSubscriptionsVar = kexpvar.NewIntSum()

	ClusterIDVar = &kexpvar.String{}
	ServerIDVar  = &kexpvar.String{}
	HostVar      = &kexpvar.String{}
	ProductVar   = &kexpvar.String{}
	VersionVar   = &kexpvar.String{}
)

var (
	startTime time.Time
)

func init() {
	startTime = time.Now().UTC()

	expvar.Publish(NumTasksVarName, NumTasksVar)
	expvar.Publish(NumEnabledTasksVarName, NumEnabledTasksVar)
	expvar.Publish(NumSubscriptionsVarName, NumSubscriptionsVar)

	expvar.Publish(ClusterIDVarName, ClusterIDVar)
	expvar.Publish(ServerIDVarName, ServerIDVar)
	expvar.Publish(HostVarName, HostVar)
	expvar.Publish(ProductVarName, ProductVar)
	expvar.Publish(VersionVarName, VersionVar)
}

func Uptime() time.Duration {
	return time.Since(startTime)
}
