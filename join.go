package kapacitor

import (
	"fmt"
	"log"
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
	// Represents the lower bound of times per group per parent
	lowMarks map[srcGroup]time.Time

	reported    map[int]bool
	allReported bool
}

// Create a new JoinNode, which takes pairs from parent streams combines them into a single point.
func newJoinNode(et *ExecutingTask, n *pipeline.JoinNode, l *log.Logger) (*JoinNode, error) {
	jn := &JoinNode{
		j:                    n,
		node:                 node{Node: n, et: et, logger: l},
		matchGroupsBuffer:    make(map[models.GroupID][]srcPoint),
		specificGroupsBuffer: make(map[models.GroupID][]srcPoint),
		lowMarks:             make(map[srcGroup]time.Time),
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
	// No more points are coming signal all groups to finish up.
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
	// Specific points may be sent to the joinset without a matching point, but not the other way around.
	// This is because the specific points have the needed specific tag data.
	// The joinset will later handle the fill inner/outer join operations.
	j.mu.Lock()
	defer j.mu.Unlock()

	if !j.allReported {
		j.reported[p.src] = true
		j.allReported = len(j.reported) == len(j.ins)
	}
	t := p.p.PointTime().Round(j.j.Tolerance)

	groupId := models.ToGroupID(
		p.p.PointName(),
		p.p.PointTags(),
		models.Dimensions{
			ByName:   p.p.PointDimensions().ByName,
			TagNames: j.j.Dimensions,
		},
	)
	// Update current srcGroup lowMark
	srcG := srcGroup{src: p.src, groupId: groupId}
	j.lowMarks[srcG] = t

	// Determine lowMark, the oldest time per parent per group.
	var lowMark time.Time
	if j.allReported {
		for s := 0; s < len(j.ins); s++ {
			sg := srcGroup{src: s, groupId: groupId}
			if lm := j.lowMarks[sg]; lowMark.IsZero() || lm.Before(lowMark) {
				lowMark = lm
			}
		}
	}

	// Check for cached specific points that can now be sent alone.
	if j.allReported {
		// Send all cached specific point that won't match anymore.
		var i int
		buf := j.specificGroupsBuffer[groupId]
		l := len(buf)
		for i = 0; i < l; i++ {
			st := buf[i].p.PointTime().Round(j.j.Tolerance)
			if st.Before(lowMark) {
				// Send point by itself since it won't get a match.
				j.sendSpecificPoint(buf[i], groupErrs)
			} else {
				break
			}
		}
		// Remove all sent points.
		j.specificGroupsBuffer[groupId] = buf[i:]
	}

	if len(p.p.PointDimensions().TagNames) > len(j.j.Dimensions) {
		// We have a specific point and three options:
		// 1. Find the cached match point and send both to group.
		// 2. Cache the specific point for later.
		// 3. Send the specific point alone if it is no longer possible that a match will arrive.

		// Search for a match.
		// Also purge any old match points.
		matches := j.matchGroupsBuffer[groupId]
		matched := false
		var i int
		l := len(matches)
		for i = 0; i < l; i++ {
			match := matches[i]
			pt := match.p.PointTime().Round(j.j.Tolerance)
			if pt.Equal(t) {
				// Option 1, send both points
				j.sendMatchPoint(p, match, groupErrs)
				matched = true
			}
			if !pt.Before(lowMark) {
				break
			}
		}
		if j.allReported {
			// Can't trust lowMark until all parents have reported.
			// Remove any unneeded match points.
			j.matchGroupsBuffer[groupId] = matches[i:]
		}

		// If the point didn't match that leaves us with options 2 and 3.
		if !matched {
			if j.allReported && t.Before(lowMark) {
				// Option 3
				// Send this specific point by itself since it won't get a match.
				j.sendSpecificPoint(p, groupErrs)
			} else {
				// Option 2
				// Cache this point for when its match arrives.
				j.specificGroupsBuffer[groupId] = append(j.specificGroupsBuffer[groupId], p)
			}
		}
	} else {
		// Cache match point.
		j.matchGroupsBuffer[groupId] = append(j.matchGroupsBuffer[groupId], p)

		// Send all specific points that match, to the group.
		var i int
		buf := j.specificGroupsBuffer[groupId]
		l := len(buf)
		for i = 0; i < l; i++ {
			st := buf[i].p.PointTime().Round(j.j.Tolerance)
			if st.Equal(t) {
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
	matched.p = np.Interface()
	group.points <- matched
}

// Send only the specific point to the group
func (j *JoinNode) sendSpecificPoint(specific srcPoint, groupErrs chan<- error) {
	group := j.getGroup(specific.p, groupErrs)
	// Send current point
	group.points <- specific
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

// A groupId and its parent
type srcGroup struct {
	src     int
	groupId models.GroupID
}

// represents an incoming data point and which parent it came from
type srcPoint struct {
	src int
	p   models.PointInterface
}

// handles emitting joined sets once enough data has arrived from parents.
type group struct {
	sets       map[time.Time][]*joinset
	head       []time.Time
	oldestTime time.Time
	j          *JoinNode
	points     chan srcPoint
}

func newGroup(i int, j *JoinNode) *group {
	return &group{
		sets:   make(map[time.Time][]*joinset),
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

	var set *joinset
	sets := g.sets[t]
	if len(sets) == 0 {
		set = newJoinset(
			g.j.j.StreamName,
			g.j.fill,
			g.j.fillValue,
			g.j.j.Names,
			g.j.j.Delimiter,
			g.j.j.Tolerance,
			t,
			g.j.logger,
		)
		sets = append(sets, set)
		g.sets[t] = sets
	}
	for j := 0; j < len(sets); j++ {
		if !sets[j].Has(i) {
			set = sets[j]
			break
		}
	}
	if set == nil {
		set = newJoinset(
			g.j.j.StreamName,
			g.j.fill,
			g.j.fillValue,
			g.j.j.Names,
			g.j.j.Delimiter,
			g.j.j.Tolerance,
			t,
			g.j.logger,
		)
		sets = append(sets, set)
		g.sets[t] = sets
	}
	set.Set(i, p)

	// Update head
	g.head[i] = t

	onlyReadySets := false
	for _, t := range g.head {
		if !t.After(g.oldestTime) {
			onlyReadySets = true
			break
		}
	}
	err := g.emit(onlyReadySets)
	if err != nil {
		return err
	}
	return nil
}

// emit a set and update the oldestTime.
func (g *group) emit(onlyReadySets bool) error {
	sets := g.sets[g.oldestTime]
	i := 0
	for ; i < len(sets); i++ {
		if sets[i].Ready() || !onlyReadySets {
			err := g.emitJoinedSet(sets[i])
			if err != nil {
				return err
			}
		} else {
			break
		}
	}
	if i == len(sets) {
		delete(g.sets, g.oldestTime)
	} else {
		g.sets[g.oldestTime] = sets[i:]
	}

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
		err := g.emit(false)
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
	delimiter string

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
	delimiter string,
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
		delimiter: delimiter,
		expected:  expected,
		values:    make([]models.PointInterface, expected),
		first:     expected,
		time:      time,
		tolerance: tolerance,
		logger:    l,
	}
}

func (js *joinset) Ready() bool {
	return js.size == js.expected
}

func (js *joinset) Has(i int) bool {
	return js.values[i] != nil
}

// add a point to the set from a given parent index.
func (js *joinset) Set(i int, v models.PointInterface) {
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
					fields[js.prefixes[i]+js.delimiter+k] = nil
				}
			case influxql.NumberFill:
				for k := range js.First().PointFields() {
					fields[js.prefixes[i]+js.delimiter+k] = js.fillValue
				}
			default:
				// inner join no valid point possible
				return models.Point{}, false
			}
		} else {
			for k, v := range p.PointFields() {
				fields[js.prefixes[i]+js.delimiter+k] = v
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
		Name:   js.name,
		Group:  js.First().PointGroup(),
		Tags:   js.First().PointTags(),
		ByName: js.First().PointDimensions().ByName,
		TMax:   js.time,
	}
	empty := make([]bool, js.expected)
	emptyCount := 0
	indexes := make([]int, js.expected)
	var fieldNames []string

BATCH_POINT:
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
			b, ok := batch.(models.Batch)
			if !ok {
				js.logger.Printf("E! invalid join data got %T expected models.Batch", batch)
				return models.Batch{}, false
			}
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
			if t.Before(setTime) {
				// We need to backup
				setTime = t
				for j := range set {
					if set[j] != nil {
						indexes[j]--
					}
					set[j] = nil
				}
				set[i] = &bp
				indexes[i]++
				count = 1
			} else if t.Equal(setTime) {
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
						fields[js.prefixes[i]+js.delimiter+k] = nil
					}
				case influxql.NumberFill:
					for _, k := range fieldNames {
						fields[js.prefixes[i]+js.delimiter+k] = js.fillValue
					}
				default:
					// inner join no valid point possible
					continue BATCH_POINT
				}
			} else {
				for k, v := range bp.Fields {
					fields[js.prefixes[i]+js.delimiter+k] = v
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
