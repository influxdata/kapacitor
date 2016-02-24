package kapacitor

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/timer"
	"github.com/influxdb/influxdb/influxql"
)

type JoinNode struct {
	node
	j             *pipeline.JoinNode
	fill          influxql.FillOption
	fillValue     interface{}
	groups        map[models.GroupID]*group
	mu            sync.RWMutex
	runningGroups sync.WaitGroup
}

// Create a new  JoinNode, which takes pairs from parent streams combines them into a single point.
func newJoinNode(et *ExecutingTask, n *pipeline.JoinNode, l *log.Logger) (*JoinNode, error) {
	for _, name := range n.Names {
		if len(name) == 0 {
			return nil, fmt.Errorf("must provide a prefix name for the join node, see .as() property method")
		}
		if strings.ContainsRune(name, '.') {
			return nil, fmt.Errorf("cannot use name %s as field prefix, it contains a '.' character", name)
		}
	}
	names := make(map[string]bool, len(n.Names))
	for _, name := range n.Names {
		if names[name] {
			return nil, fmt.Errorf("cannot use the same prefix name see .as() property method")
		}
		names[name] = true
	}

	jn := &JoinNode{
		j:    n,
		node: node{Node: n, et: et, logger: l},
	}
	// Set fill
	switch fill := n.Fill.(type) {
	case string:
		switch fill {
		case "null":
			jn.fill = influxql.NullFill
		case "none":
			jn.fill = influxql.NoFill
		default:
			return nil, fmt.Errorf("unexpected fill option %s", fill)
		}
	case int64, float64:
		jn.fill = influxql.NumberFill
		jn.fillValue = fill
	default:
		jn.fill = influxql.NoFill
	}
	jn.node.runF = jn.runJoin
	return jn, nil
}

func (j *JoinNode) runJoin([]byte) error {
	j.groups = make(map[models.GroupID]*group)
	wg := sync.WaitGroup{}
	for i := range j.ins {
		// Start gorouting per parent so we do not deadlock.
		// This way independent of the order that parents receive data
		// we can handle it.
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			in := j.ins[i]
			for p, ok := in.Next(); ok; p, ok = in.Next() {
				// Get the group for this point or batch
				g := j.getGroup(p)
				g.points <- srcPoint{
					src: i,
					p:   p,
				}
			}
		}(i)
	}
	wg.Wait()
	// No more points are comming signal all groups to finish up.
	for _, group := range j.groups {
		close(group.points)
	}
	j.runningGroups.Wait()
	for _, group := range j.groups {
		group.emitAll()
	}
	return nil
}

// safely get the group for the point or create one if it doesn't exist.
func (j *JoinNode) getGroup(p models.PointInterface) *group {
	j.mu.Lock()
	defer j.mu.Unlock()
	group := j.groups[p.PointGroup()]
	if group == nil {
		group = newGroup(len(j.ins), j)
		j.groups[p.PointGroup()] = group
		j.runningGroups.Add(1)
		go group.run()
	}
	return group
}

// represents an incoming data point and which parent it came from
type srcPoint struct {
	src int
	p   models.PointInterface
}

// handles emitting joined sets once enough data has arrived from parents.
type group struct {
	sets       map[time.Time]*joinset
	head       []time.Time
	oldestTime time.Time
	j          *JoinNode
	points     chan srcPoint
	timer      timer.Timer
}

func newGroup(i int, j *JoinNode) *group {
	return &group{
		sets:   make(map[time.Time]*joinset),
		head:   make([]time.Time, i),
		j:      j,
		points: make(chan srcPoint),
		timer:  j.et.tm.TimingService.NewTimer(j.avgExecVar),
	}
}

// start consuming incoming points
func (g *group) run() {
	defer g.j.runningGroups.Done()
	for sp := range g.points {
		g.timer.Start()
		g.collect(sp.src, sp.p)
		g.timer.Stop()
	}
}

// collect a point from a given parent.
// emit the oldest set if we have collected enough data.
func (g *group) collect(i int, p models.PointInterface) {
	t := p.PointTime().Round(g.j.j.Tolerance)
	if t.Before(g.oldestTime) || g.oldestTime.IsZero() {
		g.oldestTime = t
	}

	set := g.sets[t]
	if set == nil {
		set = newJoinset(g.j.j.StreamName, g.j.fill, g.j.fillValue, g.j.j.Names, g.j.j.Tolerance, t, g.j.logger)
		g.sets[t] = set
	}
	set.Add(i, p)

	// Update head
	g.head[i] = t

	// Check if all parents have been read past the oldestTime.
	// If so we can emit the set.
	emit := true
	for _, t := range g.head {
		if !t.After(g.oldestTime) {
			// Still posible to get more data
			// need to wait for more points
			emit = false
			break
		}
	}
	if emit {
		g.emit()
	}
}

// emit a set and update the oldestTime.
func (g *group) emit() {
	set := g.sets[g.oldestTime]
	g.emitJoinedSet(set)
	delete(g.sets, g.oldestTime)

	g.oldestTime = time.Time{}
	for t := range g.sets {
		if g.oldestTime.IsZero() || t.Before(g.oldestTime) {
			g.oldestTime = t
		}
	}
}

// emit sets until we have none left.
func (g *group) emitAll() {
	for len(g.sets) > 0 {
		g.emit()
	}
}

// emit a single joined set
func (g *group) emitJoinedSet(set *joinset) error {
	if set.name == "" {
		set.name = set.First().PointName()
	}
	switch g.j.Wants() {
	case pipeline.StreamEdge:
		p, ok := set.JoinIntoPoint()
		if ok {
			g.timer.Pause()
			for _, out := range g.j.outs {
				err := out.CollectPoint(p)
				if err != nil {
					return err
				}
			}
			g.timer.Resume()
		}
	case pipeline.BatchEdge:
		b, ok := set.JoinIntoBatch()
		if ok {
			g.timer.Pause()
			for _, out := range g.j.outs {
				err := out.CollectBatch(b)
				if err != nil {
					return err
				}
			}
			g.timer.Resume()
		}
	}
	return nil
}

// represents a set of points or batches from the same joined time
type joinset struct {
	name      string
	fill      influxql.FillOption
	fillValue interface{}
	prefixes  []string

	time      time.Time
	tolerance time.Duration
	values    []models.PointInterface

	expected int
	size     int
	finished int

	first int

	logger *log.Logger
}

func newJoinset(
	name string,
	fill influxql.FillOption,
	fillValue interface{},
	prefixes []string,
	tolerance time.Duration,
	time time.Time,
	l *log.Logger,
) *joinset {
	expected := len(prefixes)
	return &joinset{
		name:      name,
		fill:      fill,
		fillValue: fillValue,
		prefixes:  prefixes,
		expected:  expected,
		values:    make([]models.PointInterface, expected),
		first:     expected,
		time:      time,
		logger:    l,
	}
}

// add a point to the set from a given parent index.
func (js *joinset) Add(i int, v models.PointInterface) {
	if i < js.first {
		js.first = i
	}
	js.values[i] = v
	js.size++
}

// a valid point in the set
func (js *joinset) First() models.PointInterface {
	return js.values[js.first]
}

// join all points into a single point
func (js *joinset) JoinIntoPoint() (models.Point, bool) {
	fields := make(models.Fields, js.size*len(js.First().PointFields()))
	for i, p := range js.values {
		if p == nil {
			switch js.fill {
			case influxql.NullFill:
				for k := range js.First().PointFields() {
					fields[js.prefixes[i]+"."+k] = nil
				}
			case influxql.NumberFill:
				for k := range js.First().PointFields() {
					fields[js.prefixes[i]+"."+k] = js.fillValue
				}
			default:
				// inner join no valid point possible
				return models.Point{}, false
			}
		} else {
			for k, v := range p.PointFields() {
				fields[js.prefixes[i]+"."+k] = v
			}
		}
	}
	p := models.Point{
		Name:       js.name,
		Group:      js.First().PointGroup(),
		Tags:       js.First().PointTags(),
		Dimensions: js.First().PointDimensions(),
		Time:       js.time,
		Fields:     fields,
	}

	return p, true
}

// join all batches the set into a single batch
func (js *joinset) JoinIntoBatch() (models.Batch, bool) {
	newBatch := models.Batch{
		Name:  js.name,
		Group: js.First().PointGroup(),
		Tags:  js.First().PointTags(),
		TMax:  js.time,
	}
	empty := make([]bool, js.expected)
	emptyCount := 0
	indexes := make([]int, js.expected)
	var fieldNames []string
	for emptyCount < js.expected {
		set := make([]*models.BatchPoint, js.expected)
		setTime := time.Time{}
		count := 0
		for i, batch := range js.values {
			if empty[i] {
				continue
			}
			if batch == nil {
				emptyCount++
				empty[i] = true
				continue
			}
			b := batch.(models.Batch)
			if indexes[i] == len(b.Points) {
				emptyCount++
				empty[i] = true
				continue
			}
			bp := b.Points[indexes[i]]
			t := bp.Time.Round(js.tolerance)
			if setTime.IsZero() {
				setTime = t
			}
			if t.Equal(setTime) {
				if fieldNames == nil {
					for k := range bp.Fields {
						fieldNames = append(fieldNames, k)
					}
				}
				set[i] = &bp
				indexes[i]++
				count++
			}
		}
		// we didn't get any points from any group we must be empty
		// skip this set
		if count == 0 {
			continue
		}
		// Join all batch points in set
		fields := make(models.Fields, js.expected*len(fieldNames))
		for i, bp := range set {
			if bp == nil {
				switch js.fill {
				case influxql.NullFill:
					for _, k := range fieldNames {
						fields[js.prefixes[i]+"."+k] = nil
					}
				case influxql.NumberFill:
					for _, k := range fieldNames {
						fields[js.prefixes[i]+"."+k] = js.fillValue
					}
				default:
					// inner join no valid point possible
					return models.Batch{}, false
				}
			} else {
				for k, v := range bp.Fields {
					fields[js.prefixes[i]+"."+k] = v
				}
			}
		}
		bp := models.BatchPoint{
			Tags:   newBatch.Tags,
			Time:   setTime,
			Fields: fields,
		}
		newBatch.Points = append(newBatch.Points, bp)
	}
	return newBatch, true
}
