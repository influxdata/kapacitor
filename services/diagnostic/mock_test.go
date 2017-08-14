package diagnostic_test

import (
	"os"
	"testing"

	"github.com/influxdata/kapacitor/services/diagnostic"
)

func TestService(t *testing.T) {
	s := diagnostic.NewService()

	d := s.NewDiagnosticer(nil, "set", "before", "this", 1)

	l := diagnostic.NewLogger(diagnostic.NewPairEncoder(os.Stdout))

	s.SubscribeAll(l)

	for i := 0; i < 10; i++ {
		d.Diag(
			"testing", "this",
			"testing", "this",
			"testing", "this",
		)
	}

}
