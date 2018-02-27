package vars

import (
	"expvar"
	"time"

	kexpvar "github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/uuid"
)

const (
	// List of names for top-level exported vars
	ClusterIDVarName = "cluster_id"
	ServerIDVarName  = "server_id"
	HostVarName      = "host"
	ProductVarName   = "product"
	VersionVarName   = "version"
	PlatformVarName  = "platform"

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

	ClusterIDVar = &kexpvar.UUID{}
	ServerIDVar  = &kexpvar.UUID{}
	HostVar      = &kexpvar.String{}
	ProductVar   = &kexpvar.String{}
	VersionVar   = &kexpvar.String{}
	PlatformVar  = &kexpvar.String{}
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
	expvar.Publish(PlatformVarName, PlatformVar)
}

func uptime() time.Duration {
	return time.Since(startTime)
}

type Infoer interface {
	ClusterID() uuid.UUID
	ServerID() uuid.UUID
	Hostname() string
	Version() string
	Product() string
	Platform() string
	NumTasks() int64
	NumEnabledTasks() int64
	NumSubscriptions() int64
	Uptime() time.Duration
}

var Info = info{}

type info struct{}

func (info) ClusterID() uuid.UUID {
	return ClusterIDVar.UUIDValue()
}
func (info) ServerID() uuid.UUID {
	return ServerIDVar.UUIDValue()
}
func (info) Hostname() string {
	return HostVar.StringValue()
}
func (info) Version() string {
	return VersionVar.StringValue()
}
func (info) Product() string {
	return ProductVar.StringValue()
}
func (info) Platform() string {
	return PlatformVar.StringValue()
}

func (info) NumTasks() int64 {
	return NumTasksVar.IntValue()
}
func (info) NumEnabledTasks() int64 {
	return NumEnabledTasksVar.IntValue()
}
func (info) NumSubscriptions() int64 {
	return NumSubscriptionsVar.IntValue()
}
func (info) Uptime() time.Duration {
	return uptime()
}
