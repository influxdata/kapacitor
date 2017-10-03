package pipeline

/*
func TestQueryNode(t *testing.T) {
	want := newQueryNode().
		GroupBy("host").
		GroupByMeasurement().
		Align().
		AlignGroup()
	want.QueryStr = (`select * from "telegraf"."autogen"`)
	want.Period = time.Second
	want.Every = time.Second
	want.Cron = "0 0 29 2 *"
	want.Offset = time.Second
	want.Fill = 1.0
	want.Cluster = "string"

	b, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("TestQueryNode error while marshaling to JSON: %v", err)
	}
	if b == nil {
		t.Fatal("TestQueryNode error while marshaling to JSON: produced nil")
	}

	var got QueryNode
	if err = json.Unmarshal(b, &got); err != nil {
		t.Fatalf("TestQueryNode error while unmarshaling JSON: %v", err)
	}

	opts := []cmp.Option{cmp.AllowUnexported(QueryNode{}), cmpopts.IgnoreUnexported(QueryNode{})}
	if !cmp.Equal(*want, got, opts...) {
		t.Errorf("TestQueryNode error in JSON serialization. -got/+want\n%s", cmp.Diff(*want, got, opts...))
	}
}

func TestQueryNodeTick(t *testing.T) {
	n := newQueryNode().
		GroupBy("host").
		GroupByMeasurement().
		Align().
		AlignGroup()
	n.QueryStr = (`select * from "telegraf"."autogen"`)
	n.Period = time.Second
	n.Every = time.Second
	n.Cron = "0 0 29 2 *"
	n.Offset = time.Second
	n.Fill = 1.0
	n.Cluster = "string"

	var buf bytes.Buffer
	n.Tick(&buf)
	got := buf.String()
	want := `|query('''select * from "telegraf"."autogen"''').period(1s).every(1s).align().cron('0 0 29 2 *').offset(1s).alignGroup().groupBy('host').groupByMeasurement().fill(1.000000).cluster('string')`
	if got != want {
		t.Errorf("TestQueryNodeTick() = got:\n%s\nwant:\n%s\n", got, want)
	}
}

func TestBatchNodeTick(t *testing.T) {
	p := &Pipeline{}
	scope := stateful.NewScope()
	src := newBatchNode()
	scope.Set("batch", src)
	p.addSource(src)

	q := src.Query(`select * from "telegraf"."autogen"`)
	q.GroupBy("host").
		GroupByMeasurement().
		Align().
		AlignGroup()
	q.Period = time.Second
	q.Every = time.Second
	q.Cron = "0 0 29 2 *"
	q.Offset = time.Second
	q.Fill = 1.0
	q.Cluster = "string"

	var buf bytes.Buffer
	src.Tick(&buf)
	got := buf.String()
	want := `batch|query('''select * from "telegraf"."autogen"''').period(1s).every(1s).align().cron('0 0 29 2 *').offset(1s).alignGroup().groupBy('host').groupByMeasurement().fill(1.000000).cluster('string')`
	if got != want {
		t.Errorf("TestBatchNodeTick() = got:\n%s\nwant:\n%s\n", got, want)
	}
}

func TestPipelineBatchTick(t *testing.T) {
	script := `batch|query('''select * from "telegraf"."autogen"''').period(1h).every(1s).align().cron('0 0 29 2 *').offset(1s).alignGroup().groupBy(*, 'host').groupByMeasurement().fill('null').cluster('string')`

	scope := stateful.NewScope()
	p, err := CreatePipeline(script, BatchEdge, scope, deadman{}, nil)
	if err != nil {
		t.Fatalf("Error creating pipeline %v", err)
	}

	var buf bytes.Buffer
	visit := false
	p.Walk(func(n Node) error {
		if visit {
			return nil
		}
		n.Tick(&buf)
		visit = true
		return nil
	})

	got := buf.String()
	want := script

	if got != want {
		t.Errorf("TestPipelineBatchTick() = got:\n%s\nwant:\n%s\n", got, want)
	}
}
*/

/*
func TestCPG(t *testing.T) {
	var script = `stream
    		   |from()
    		       .database('telegraf')
    		       .retentionPolicy('autogen')
    		       .measurement('cpu')
    		       .groupByMeasurement()
    		       .groupBy([*])
    		       .where(lambda: "cpu" != 'cpu-total' AND "host" =~ /logger\d+/)`

	want := "howdy"
	scope := stateful.NewScope()
	p, err := CreatePipeline(script, StreamEdge, scope, deadman{}, nil)
	if err != nil {
		t.Fatalf("Error creating pipeline %v", err)
	}

	var buf bytes.Buffer
	visit := false
	p.Walk(func(n Node) error {
		if visit {
			return nil
		}
		n.Tick(&buf)
		visit = true
		return nil
	})

	got := buf.String()

	if got != want {
		t.Errorf("TestCPG() = got:\n%s\nwant:\n%s\n", got, want)
	}
}
*/
