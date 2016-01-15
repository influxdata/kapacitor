package kapacitor

import (
	"expvar"
	"runtime"
	"strconv"
	"sync"
	"time"

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
	NumTasks         = &expvar.Int{}
	NumEnabledTasks  = &expvar.Int{}
	NumSubscriptions = &expvar.Int{}
)

var (
	startTime time.Time
)

func init() {
	startTime = time.Now().UTC()
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

func Uptime() time.Duration {
	return time.Now().Sub(startTime)
}

var expvarMu sync.Mutex

// NewStatistics creates an expvar-based map. Within there "name" is the Measurement name, "tags" are the tags,
// and values are placed at the key "values".
// The "values" map is returned so that statistics can be set.
func NewStatistics(name string, tags map[string]string) *expvar.Map {
	expvarMu.Lock()
	defer expvarMu.Unlock()

	key := uuid.NewV4().String()

	m := &expvar.Map{}
	m.Init()
	expvar.Publish(key, m)

	// Set the name
	nameVar := &expvar.String{}
	nameVar.Set(name)
	m.Set("name", nameVar)

	// Set the tags
	tagsVar := &expvar.Map{}
	tagsVar.Init()
	for k, v := range tags {
		value := &expvar.String{}
		value.Set(v)
		tagsVar.Set(k, value)
	}
	m.Set("tags", tagsVar)

	// Create and set the values entry used for actual stats.
	statMap := &expvar.Map{}
	statMap.Init()
	m.Set("values", statMap)

	return statMap
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

	expvar.Do(func(kv expvar.KeyValue) {
		var f interface{}
		var err error
		switch v := kv.Value.(type) {
		case *expvar.Float:
			f, err = strconv.ParseFloat(v.String(), 64)
			if err == nil {
				globalData.Values[kv.Key] = f
			}
		case *expvar.Int:
			f, err = strconv.ParseInt(v.String(), 10, 64)
			if err == nil {
				globalData.Values[kv.Key] = f
			}
		case *expvar.Map:
			data := StatsData{
				Tags:   make(map[string]string),
				Values: make(map[string]interface{}),
			}

			v.Do(func(subKV expvar.KeyValue) {
				switch subKV.Key {
				case "name":
					// straight to string name.
					u, err := strconv.Unquote(subKV.Value.String())
					if err != nil {
						return
					}
					data.Name = u
				case "tags":
					// string-string tags map.
					n := subKV.Value.(*expvar.Map)
					n.Do(func(t expvar.KeyValue) {
						u, err := strconv.Unquote(t.Value.String())
						if err != nil {
							return
						}
						data.Tags[t.Key] = u
					})
				case "values":
					// string-interface map.
					n := subKV.Value.(*expvar.Map)
					n.Do(func(kv expvar.KeyValue) {
						var f interface{}
						var err error
						switch v := kv.Value.(type) {
						case *expvar.Float:
							f, err = strconv.ParseFloat(v.String(), 64)
							if err != nil {
								return
							}
						case *expvar.Int:
							f, err = strconv.ParseInt(v.String(), 10, 64)
							if err != nil {
								return
							}
						default:
							return
						}
						data.Values[kv.Key] = f
					})
				}
			})

			// If no field data, don't include it in the results
			if len(data.Values) == 0 {
				return
			}

			allData = append(allData, data)
		}
	})

	// Add uptime to globalData
	globalData.Values[UptimeVarName] = Uptime().Seconds()

	// Add Go memstats.
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
