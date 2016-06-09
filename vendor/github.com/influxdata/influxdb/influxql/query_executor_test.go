package influxql_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/influxdata/influxdb/influxql"
)

var errUnexpected = errors.New("unexpected error")

type StatementExecutor struct {
	ExecuteStatementFn func(stmt influxql.Statement, ctx *influxql.ExecutionContext) error
}

func (e *StatementExecutor) ExecuteStatement(stmt influxql.Statement, ctx *influxql.ExecutionContext) error {
	return e.ExecuteStatementFn(stmt, ctx)
}

func (e *StatementExecutor) NormalizeStatement(stmt influxql.Statement, database string) error {
	return nil
}

func TestQueryExecutor_AttachQuery(t *testing.T) {
	q, err := influxql.ParseQuery(`SELECT count(value) FROM cpu`)
	if err != nil {
		t.Fatal(err)
	}

	e := influxql.NewQueryExecutor()
	e.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *influxql.ExecutionContext) error {
			if ctx.QueryID != 1 {
				t.Errorf("incorrect query id: exp=1 got=%d", ctx.QueryID)
			}
			return nil
		},
	}

	discardOutput(e.ExecuteQuery(q, "mydb", 100, false, nil))
}

func TestQueryExecutor_KillQuery(t *testing.T) {
	q, err := influxql.ParseQuery(`SELECT count(value) FROM cpu`)
	if err != nil {
		t.Fatal(err)
	}

	qid := make(chan uint64)

	e := influxql.NewQueryExecutor()
	e.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *influxql.ExecutionContext) error {
			qid <- ctx.QueryID
			select {
			case <-ctx.InterruptCh:
				return influxql.ErrQueryInterrupted
			case <-time.After(100 * time.Millisecond):
				t.Error("killing the query did not close the channel after 100 milliseconds")
				return errUnexpected
			}
		},
	}

	results := e.ExecuteQuery(q, "mydb", 100, false, nil)
	q, err = influxql.ParseQuery(fmt.Sprintf("KILL QUERY %d", <-qid))
	if err != nil {
		t.Fatal(err)
	}
	discardOutput(e.ExecuteQuery(q, "mydb", 100, false, nil))

	result := <-results
	if result.Err != influxql.ErrQueryInterrupted {
		t.Errorf("unexpected error: %s", result.Err)
	}
}

func TestQueryExecutor_Interrupt(t *testing.T) {
	q, err := influxql.ParseQuery(`SELECT count(value) FROM cpu`)
	if err != nil {
		t.Fatal(err)
	}

	e := influxql.NewQueryExecutor()
	e.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *influxql.ExecutionContext) error {
			select {
			case <-ctx.InterruptCh:
				return influxql.ErrQueryInterrupted
			case <-time.After(100 * time.Millisecond):
				t.Error("killing the query did not close the channel after 100 milliseconds")
				return errUnexpected
			}
		},
	}

	closing := make(chan struct{})
	results := e.ExecuteQuery(q, "mydb", 100, false, closing)
	close(closing)
	result := <-results
	if result.Err != influxql.ErrQueryInterrupted {
		t.Errorf("unexpected error: %s", result.Err)
	}
}

func TestQueryExecutor_ShowQueries(t *testing.T) {
	q, err := influxql.ParseQuery(`SELECT count(value) FROM cpu`)
	if err != nil {
		t.Fatal(err)
	}

	e := influxql.NewQueryExecutor()
	e.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *influxql.ExecutionContext) error {
			t.Errorf("unexpected statement: %s", stmt)
			return errUnexpected
		},
	}

	q, err = influxql.ParseQuery(`SHOW QUERIES`)
	if err != nil {
		t.Fatal(err)
	}

	results := e.ExecuteQuery(q, "", 100, false, nil)
	result := <-results
	if len(result.Series) != 1 {
		t.Errorf("expected %d rows, got %d", 1, len(result.Series))
	}
	if result.Err != nil {
		t.Errorf("unexpected error: %s", result.Err)
	}
}

func TestQueryExecutor_Limit_Timeout(t *testing.T) {
	q, err := influxql.ParseQuery(`SELECT count(value) FROM cpu`)
	if err != nil {
		t.Fatal(err)
	}

	e := influxql.NewQueryExecutor()
	e.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *influxql.ExecutionContext) error {
			select {
			case <-ctx.InterruptCh:
				return influxql.ErrQueryInterrupted
			case <-time.After(time.Second):
				t.Errorf("timeout has not killed the query")
				return errUnexpected
			}
		},
	}
	e.QueryTimeout = time.Nanosecond

	results := e.ExecuteQuery(q, "mydb", 100, false, nil)
	result := <-results
	if result.Err != influxql.ErrQueryTimeoutReached {
		t.Errorf("unexpected error: %s", result.Err)
	}
}

func TestQueryExecutor_Limit_ConcurrentQueries(t *testing.T) {
	q, err := influxql.ParseQuery(`SELECT count(value) FROM cpu`)
	if err != nil {
		t.Fatal(err)
	}

	qid := make(chan uint64)

	e := influxql.NewQueryExecutor()
	e.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *influxql.ExecutionContext) error {
			qid <- ctx.QueryID
			<-ctx.InterruptCh
			return influxql.ErrQueryInterrupted
		},
	}
	e.MaxConcurrentQueries = 1
	defer e.Close()

	// Start first query and wait for it to be executing.
	go discardOutput(e.ExecuteQuery(q, "mydb", 100, false, nil))
	<-qid

	// Start second query and expect for it to fail.
	results := e.ExecuteQuery(q, "mydb", 100, false, nil)

	select {
	case result := <-results:
		if len(result.Series) != 0 {
			t.Errorf("expected %d rows, got %d", 0, len(result.Series))
		}
		if result.Err != influxql.ErrMaxConcurrentQueriesReached {
			t.Errorf("unexpected error: %s", result.Err)
		}
	case <-qid:
		t.Errorf("unexpected statement execution for the second query")
	}
}

func TestQueryExecutor_Close(t *testing.T) {
	q, err := influxql.ParseQuery(`SELECT count(value) FROM cpu`)
	if err != nil {
		t.Fatal(err)
	}

	ch1 := make(chan struct{})
	ch2 := make(chan struct{})

	e := influxql.NewQueryExecutor()
	e.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *influxql.ExecutionContext) error {
			close(ch1)
			<-ctx.InterruptCh
			close(ch2)
			return influxql.ErrQueryInterrupted
		},
	}

	results := e.ExecuteQuery(q, "mydb", 100, false, nil)
	go func(results <-chan *influxql.Result) {
		result := <-results
		if result.Err != influxql.ErrQueryEngineShutdown {
			t.Errorf("unexpected error: %s", result.Err)
		}
	}(results)

	// Wait for the statement to start executing.
	<-ch1

	// Close the query executor.
	e.Close()

	// Check that the statement gets interrupted and finishes.
	select {
	case <-ch2:
	case <-time.After(100 * time.Millisecond):
		t.Error("closing the query manager did not kill the query after 100 milliseconds")
	}

	results = e.ExecuteQuery(q, "mydb", 100, false, nil)
	result := <-results
	if len(result.Series) != 0 {
		t.Errorf("expected %d rows, got %d", 0, len(result.Series))
	}
	if result.Err != influxql.ErrQueryEngineShutdown {
		t.Errorf("unexpected error: %s", result.Err)
	}
}

func TestQueryExecutor_Panic(t *testing.T) {
	q, err := influxql.ParseQuery(`SELECT count(value) FROM cpu`)
	if err != nil {
		t.Fatal(err)
	}

	e := influxql.NewQueryExecutor()
	e.StatementExecutor = &StatementExecutor{
		ExecuteStatementFn: func(stmt influxql.Statement, ctx *influxql.ExecutionContext) error {
			panic("test error")
		},
	}

	results := e.ExecuteQuery(q, "mydb", 100, false, nil)
	result := <-results
	if len(result.Series) != 0 {
		t.Errorf("expected %d rows, got %d", 0, len(result.Series))
	}
	if result.Err == nil || result.Err.Error() != "SELECT count(value) FROM cpu [panic:test error]" {
		t.Errorf("unexpected error: %s", result.Err)
	}
}

func discardOutput(results <-chan *influxql.Result) {
	for range results {
		// Read all results and discard.
	}
}
