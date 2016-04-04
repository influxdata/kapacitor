package kapacitor

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/timer"
)

type JoinNode struct {
	node
	j             *pipeline.JoinNode
	fill          influxql.FillOption
	fillValue     interface{}
	groups        map[models.GroupID]*group
	mu            sync.RWMutex
	runningGroups sync.WaitGroup

	// Buffer for caching points that need to be matched with specific points.
	matchGroupsBuffer map[models.GroupID][]srcPoint
	// Buffer for caching specific points until their match arrivces.
	specificGroupsBuffer map[models.GroupID][]srcPoint
	// Represents the lower bound of times from each parent.
	lowMark     time.Time
	reported    map[int]bool
	allReported bool
}

// Create a new JoinNode, which takes pairs from parent streams combines them into a single point.
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
		j:                    n,
		node:                 node{Node: n, et: et, logger: l},
		matchGroupsBuffer:    make(map[models.GroupID][]srcPoint),
		specificGroupsBuffer: make(map[models.GroupID][]srcPoint),
		reported:             make(map[int]bool),
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

	groupErrs := make(chan error, 1)
	done := make(chan struct{}, len(j.ins))

	for i := range j.ins {
		// Start gorouting per parent so we do not deadlock.
		// This way independent of the order that parents receive data
		// we can handle it.
		t := j.et.tm.TimingService.NewTimer(j.statMap.Get(statAverageExecTime).(timer.Setter))
		go func(i int, t timer.Timer) {
			defer func() {
				done <- struct{}{}
			}()
			in := j.ins[i]
			for p, ok := in.Next(); ok; p, ok = in.Next() {
				t.Start()
				srcP := srcPoint{src: i, p: p}
				if len(j.j.Dimensions) > 0 {
					// Match points with their group based on join dimensions.
					j.matchPoints(srcP, groupErrs)
				} else {
					// Just send point on to group, we are not joining on specific dimensions.
					func() {
						j.mu.Lock()
						defer j.mu.Unlock()
						group := j.getGroup(p, groupErrs)
						// Send current point
						group.points <- srcP
					}()
				}
				t.Stop()
			}
		}(i, t)
	}
	for range j.ins {
		select {
		case <-done:
		case err := <-groupErrs:
			return err
		}
	}
	// No more points are comming signal all groups to finish up.
	for _, group := range j.groups {
		close(group.points)
	}
	j.runningGroups.Wait()
	for _, group := range j.groups {
		err := group.emitAll()
		if err != nil {
			return err
		}
	}
	return nil
}

// The purpose of this method is to match more specific points
// with the less specific points as they arrive.
//
// Where 'more specific' means, that a point has more dimensions than the join.on dimensions.
func (j *JoinNode) matchPoints(p srcPoint, groupErrs chan<- error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	if !j.allReported {
		j.reported[p.src] = true
		j.allReported = len(j.reported) == len(j.ins)
	}
	t := p.p.PointTime().Round(j.j.Tolerance)
	if j.lowMark.IsZero() || t.Before(j.lowMark) {
		j.lowMark = t
	}

	groupId := models.TagsToGroupID(j.j.Dimensions, p.p.PointTags())
	if len(p.p.PointDimensions()) > len(j.j.Dimensions) {
		// We have a specific point, find its cached match and send both to group
		matches := j.matchGroupsBuffer[groupId]
		matched := false
		for _, match := range matches {
			if match.p.PointTime().Round(j.j.Tolerance).Equal(t) {
				j.sendMatchPoint(p, match, groupErrs)
				matched = true
			}
		}
		if !matched {
			// Cache this point for when its match arrives
			j.specificGroupsBuffer[groupId] = append(j.specificGroupsBuffer[groupId], p)
		}

		// Purge cached match points
		if j.allReported {
			for id, cached := range j.matchGroupsBuffer {
				var i int
				l := len(cached)
				for i = 0; i < l; i++ {
					if !cached[i].p.PointTime().Round(j.j.Tolerance).Before(j.lowMark) {
						break
					}
				}
				j.matchGroupsBuffer[id] = cached[i:]
			}
		}

	} else {

		// Cache match point.
		j.matchGroupsBuffer[groupId] = append(j.matchGroupsBuffer[groupId], p)

		// Send all specific points, that match, to the group.
		var i int
		buf := j.specificGroupsBuffer[groupId]
		l := len(buf)
		for i = 0; i < l; i++ {
			if buf[i].p.PointTime().Round(j.j.Tolerance).Equal(t) {
				j.sendMatchPoint(buf[i], p, groupErrs)
			} else {
				break
			}
		}
		// Remove all sent points
		j.specificGroupsBuffer[groupId] = buf[i:]
	}
}

// Add the specific tags from the specific point to the matched point
// and then send both on to the group.
func (j *JoinNode) sendMatchPoint(specific, matched srcPoint, groupErrs chan<- error) {
	np := matched.p.Copy().Setter()
	for key, value := range specific.p.PointTags() {
		np.SetNewDimTag(key, value)
	}
	np.UpdateGroup()
	group := j.getGroup(specific.p, groupErrs)
	// Send current point
	group.points <- specific
	// Send new matched point
	matched.p = np
	group.points <- matched
}

// safely get the group for the point or create one if it doesn't exist.
func (j *JoinNode) getGroup(p models.PointInterface, groupErrs chan<- error) *group {
	group := j.groups[p.PointGroup()]
	if group == nil {
		group = newGroup(len(j.ins), j)
		j.groups[p.PointGroup()] = group
		j.runningGroups.Add(1)
		go func() {
			err := group.run()
			if err != nil {
				j.logger.Println("E! join group error:", err)
				select {
				case groupErrs <- err:
				default:
				}
			}
		}()
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
}

func newGroup(i int, j *JoinNode) *group {
	return &group{
		sets:   make(map[time.Time]*joinset),
		head:   make([]time.Time, i),
		j:      j,
		points: make(chan srcPoint),
	}
}

// start consuming incoming points
func (g *group) run() error {
	defer g.j.runningGroups.Done()
	for sp := range g.points {
		err := g.collect(sp.src, sp.p)
		if err != nil {
			return err
		}
	}
	return nil
}

// collect a point from a given parent.
// emit the oldest set if we have collected enough data.
func (g *group) collect(i int, p models.PointInterface) error {
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
		err := g.emit()
		if err != nil {
			return err
		}
	}
	return nil
}

// emit a set and update the oldestTime.
func (g *group) emit() error {
	set := g.sets[g.oldestTime]
	err := g.emitJoinedSet(set)
	if err != nil {
		return err
	}
	delete(g.sets, g.oldestTime)

	g.oldestTime = time.Time{}
	for t := range g.sets {
		if g.oldestTime.IsZero() || t.Before(g.oldestTime) {
			g.oldestTime = t
		}
	}
	return nil
}

// emit sets until we have none left.
func (g *group) emitAll() error {
	var lastErr error
	for len(g.sets) > 0 {
		err := g.emit()
		if err != nil {
			lastErr = err
		}
	}
	return lastErr
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
			for _, out := range g.j.outs {
				err := out.CollectPoint(p)
				if err != nil {
					return err
				}
			}
		}
	case pipeline.BatchEdge:
		b, ok := set.JoinIntoBatch()
		if ok {
			for _, out := range g.j.outs {
				err := out.CollectBatch(b)
				if err != nil {
					return err
				}
			}
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
		tolerance: tolerance,
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

type durationVar struct {
	expvar.Int
}

func (d *durationVar) String() string {
	return time.Duration(d.IntValue()).String()
}
