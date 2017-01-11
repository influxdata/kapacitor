package kapacitor

import (
	"expvar"
	"fmt"
	"runtime"
	"time"

	kexpvar "github.com/influxdata/kapacitor/expvar"
	"github.com/twinj/uuid"
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

	// All internal stats are added as sub-maps to this top level map.
	stats *kexpvar.Map
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

	// Initialze the global stats map
	stats = &kexpvar.Map{}
	stats.Init()
	expvar.Publish(Product, stats)
}

func Uptime() time.Duration {
	return time.Since(startTime)
}

// NewStatistics creates an expvar-based map. Within there "name" is the Measurement name, "tags" are the tags,
// and values are placed at the key "values".
// The "values" map is returned so that statistics can be set.
func NewStatistics(name string, tags map[string]string) (string, *kexpvar.Map) {
	key := uuid.NewV4().String()

	m := &kexpvar.Map{}
	m.Init()

	// Set the name
	nameVar := &kexpvar.String{}
	nameVar.Set(name)
	m.Set("name", nameVar)

	// Set the tags
	tagsVar := &kexpvar.Map{}
	tagsVar.Init()
	for k, v := range tags {
		value := &kexpvar.String{}
		value.Set(v)
		tagsVar.Set(k, value)
	}
	// Always add ID tags
	tagsVar.Set(ClusterIDVarName, ClusterIDVar)
	tagsVar.Set(ServerIDVarName, ServerIDVar)
	tagsVar.Set(HostVarName, HostVar)

	m.Set("tags", tagsVar)

	// Create and set the values entry used for actual stats.
	statMap := &kexpvar.Map{}
	statMap.Init()
	m.Set("values", statMap)

	// Set new statsMap on the top level map.
	stats.Set(key, m)

	return key, statMap
}

// Remove a statistics map.
func DeleteStatistics(key string) {
	stats.Delete(key)
}

type StatsData struct {
	Name   string                 `json:"name"`
	Tags   map[string]string      `json:"tags"`
	Values map[string]interface{} `json:"values"`
}

// Return all stats data from the expvars.
func GetStatsData() ([]StatsData, error) {
	allData := make([]StatsData, 0)
	// Add Global expvars
	globalData := StatsData{
		Name:   "kapacitor",
		Values: make(map[string]interface{}),
	}

	allData = append(allData, globalData)

	// Get all global statistics
	expvar.Do(func(kv expvar.KeyValue) {
		switch v := kv.Value.(type) {
		case kexpvar.IntVar:
			globalData.Values[kv.Key] = v.IntValue()
		case kexpvar.FloatVar:
			globalData.Values[kv.Key] = v.FloatValue()
		case *kexpvar.Map:
			if kv.Key != Product {
				panic("unexpected published top level expvar.Map with key " + kv.Key)
			}
		}
	})
	// Get all other specific statistics
	stats.Do(func(kv expvar.KeyValue) {
		v := kv.Value.(*kexpvar.Map)

		data := StatsData{
			Tags:   make(map[string]string),
			Values: make(map[string]interface{}),
		}

		v.Do(func(subKV expvar.KeyValue) {
			switch subKV.Key {
			case "name":
				data.Name = subKV.Value.(*kexpvar.String).StringValue()
			case "tags":
				// string-string tags map.
				n := subKV.Value.(*kexpvar.Map)
				n.Do(func(t expvar.KeyValue) {
					data.Tags[t.Key] = t.Value.(*kexpvar.String).StringValue()
				})
			case "values":
				// string-interface map.
				n := subKV.Value.(*kexpvar.Map)
				n.Do(func(kv expvar.KeyValue) {
					switch v := kv.Value.(type) {
					case kexpvar.IntVar:
						data.Values[kv.Key] = v.IntValue()
					case kexpvar.FloatVar:
						data.Values[kv.Key] = v.FloatValue()
					default:
						panic(fmt.Sprintf("unknown expvar.Var type for stats %T", kv.Value))
					}
				})
			}
		})

		// If no field data, don't include it in the results
		if len(data.Values) == 0 {
			return
		}

		allData = append(allData, data)
	})

	// Add uptime to globalData
	globalData.Values[UptimeVarName] = Uptime().Seconds()

	// Add Go runtime stats.
	data := StatsData{
		Name: "runtime",
	}

	var rt runtime.MemStats
	runtime.ReadMemStats(&rt)
	data.Values = map[string]interface{}{
		"Alloc":        int64(rt.Alloc),
		"TotalAlloc":   int64(rt.TotalAlloc),
		"Sys":          int64(rt.Sys),
		"Lookups":      int64(rt.Lookups),
		"Mallocs":      int64(rt.Mallocs),
		"Frees":        int64(rt.Frees),
		"HeapAlloc":    int64(rt.HeapAlloc),
		"HeapSys":      int64(rt.HeapSys),
		"HeapIdle":     int64(rt.HeapIdle),
		"HeapInUse":    int64(rt.HeapInuse),
		"HeapReleased": int64(rt.HeapReleased),
		"HeapObjects":  int64(rt.HeapObjects),
		"PauseTotalNs": int64(rt.PauseTotalNs),
		"NumGC":        int64(rt.NumGC),
		"NumGoroutine": int64(runtime.NumGoroutine()),
	}
	allData = append(allData, data)

	return allData, nil
}
