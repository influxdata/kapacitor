// Sends anonymous reports to InfluxData
package reporting

import (
	"expvar"
	"log"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/influxdb/enterprise-client/v1"
	"github.com/influxdb/kapacitor"
)

const reportingInterval = time.Hour * 12

// Sends periodic information to Enterprise.
// If not registered with Enterprise just
// registers the server on startup and sends anonymous
// stats every 12 hours.
//
// If registered with Enterprise also sends
// all expvar statistics at the Config.StatsInterval.
type Service struct {
	tags client.Tags

	client *client.Client

	clusterID string
	serverID  string
	hostname  string
	version   string
	product   string

	statsInterval time.Duration
	statsTicker   *time.Ticker
	usageTicker   *time.Ticker
	closing       chan struct{}
	logger        *log.Logger
	wg            sync.WaitGroup
}

func NewService(c Config, token string, l *log.Logger) *Service {
	client := client.New(token)
	client.URL = c.EnterpriseURL
	return &Service{
		client:        client,
		logger:        l,
		statsInterval: time.Duration(c.StatsInterval),
	}
}

func (s *Service) Open() error {
	if s.closing == nil {
		s.closing = make(chan struct{})
	}

	// Populate published vars
	s.clusterID = kapacitor.GetStringVar(kapacitor.ClusterIDVarName)
	s.serverID = kapacitor.GetStringVar(kapacitor.ServerIDVarName)
	s.hostname = kapacitor.GetStringVar(kapacitor.HostVarName)
	s.version = kapacitor.GetStringVar(kapacitor.VersionVarName)
	s.product = kapacitor.Product

	// Populate anonymous tags
	s.tags = make(client.Tags)
	s.tags["version"] = s.version
	s.tags["arch"] = runtime.GOARCH
	s.tags["os"] = runtime.GOOS

	// Check for enterprise token
	if s.client.Token == "" {
		r := client.Registration{
			ClusterID: s.clusterID,
			Product:   s.product,
		}
		u, _ := s.client.RegistrationURL(r)
		s.logger.Println("E! No Enterprise token configured, please register at", u)
	} else {
		// Send periodic stats
		s.statsTicker = time.NewTicker(s.statsInterval)
		s.wg.Add(1)
		go s.stats()
	}

	// Register server on startup
	err := s.registerServer()
	if err != nil {
		s.logger.Println("E! error registering server:", err)
	}

	// Send anonymous usage stats on startup
	s.usageTicker = time.NewTicker(reportingInterval)
	err = s.sendUsageReport()
	if err != nil {
		s.logger.Println("E! error sending usage stats:", err)
	}

	// Send periodic anonymous usage stats
	s.wg.Add(1)
	go s.usage()
	return nil
}

func (s *Service) Close() error {
	if s.usageTicker != nil {
		s.usageTicker.Stop()
	}
	if s.statsTicker != nil {
		s.statsTicker.Stop()
	}
	if s.closing != nil {
		close(s.closing)
	}
	s.wg.Wait()
	return nil
}

func (s *Service) usage() {
	defer s.wg.Done()
	for {
		select {
		case <-s.closing:
			return
		case <-s.usageTicker.C:
			err := s.sendUsageReport()
			if err != nil {
				s.logger.Println("E! error while sending usage report:", err)
			}
		}
	}
}

func (s *Service) stats() {
	defer s.wg.Done()
	for {
		select {
		case <-s.closing:
			return
		case <-s.statsTicker.C:
			err := s.sendStatsReport()
			if err != nil {
				s.logger.Println("E! error while sending stats report:", err)
			}
		}
	}
}

// Register this server with Enterprise.
func (s *Service) registerServer() error {
	server := client.Server{
		ClusterID: s.clusterID,
		ServerID:  s.serverID,
		Host:      s.hostname,
		Version:   s.version,
		Product:   s.product,
	}
	resp, err := s.client.Save(server)
	if resp != nil {
		resp.Body.Close()
	}
	return err
}

// Send anonymous usage report.
func (s *Service) sendUsageReport() error {
	data := client.UsageData{
		Tags:   s.tags,
		Values: make(client.Values),
	}
	// Add values
	data.Values[kapacitor.ClusterIDVarName] = s.clusterID
	data.Values[kapacitor.ServerIDVarName] = s.serverID
	data.Values[kapacitor.NumTasksVarName] = kapacitor.GetIntVar(kapacitor.NumTasksVarName)
	data.Values[kapacitor.NumEnabledTasksVarName] = kapacitor.GetIntVar(kapacitor.NumEnabledTasksVarName)
	data.Values[kapacitor.NumSubscriptionsVarName] = kapacitor.GetIntVar(kapacitor.NumSubscriptionsVarName)

	usage := client.Usage{
		Product: kapacitor.Product,
		Data:    []client.UsageData{data},
	}

	resp, err := s.client.Save(usage)
	if resp != nil {
		resp.Body.Close()
	}
	return err
}

// Send all internal stats.
func (s *Service) sendStatsReport() error {
	data, err := s.getStatsData()
	if err != nil {
		return err
	}
	stats := client.Stats{
		ClusterID: s.clusterID,
		ServerID:  s.serverID,
		Product:   s.product,
		Data:      data,
	}

	resp, err := s.client.Save(stats)
	if resp != nil {
		resp.Body.Close()
	}
	return err
}

// Return all stats data from the expvars.
func (s *Service) getStatsData() ([]client.StatsData, error) {
	allData := make([]client.StatsData, 0)
	// Add Global expvars
	globalData := client.StatsData{
		Name:   "kapacitor",
		Values: make(client.Values),
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
			data := client.StatsData{
				Tags:   make(client.Tags),
				Values: make(client.Values),
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

			// If a registered client has no field data, don't include it in the results
			if len(data.Values) == 0 {
				return
			}

			allData = append(allData, data)
		}
	})

	// Add Go memstats.
	data := client.StatsData{
		Name: "runtime",
	}

	var rt runtime.MemStats
	runtime.ReadMemStats(&rt)
	data.Values = client.Values{
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
