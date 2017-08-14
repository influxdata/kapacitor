package notary_test

import (
	"io/ioutil"
	"testing"

	"github.com/influxdata/kapacitor/services/notary"
)

func BenchmarkWithoutContext(b *testing.B) {
	b.Run("Notary.Info", func(b *testing.B) {
		n := notary.WithContext(notary.NewPairLogger(ioutil.Discard))
		//n := notary.WithContext(notary.NewPairLogger(os.Stdout))
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				n.Info(
					"msg", "idk",
					"test", 1,
					"msg", "idk",
					"test", 1,
					"msg", "idk",
					"test", 1,
					"msg", "idk",
					"test", 1,
					"msg", "idk",
					"test", 1,
					"msg", "idk",
					"test", 1,
				)
			}
		})
	})
	b.Run("Notary.Error", func(b *testing.B) {
		n := notary.WithContext(notary.NewPairLogger(ioutil.Discard))
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				n.Error(
					"msg", "idk",
					"test", 1,
					"msg", "idk",
					"test", 1,
					"msg", "idk",
					"test", 1,
					"msg", "idk",
					"test", 1,
					"msg", "idk",
					"test", 1,
					"msg", "idk",
					"test", 1,
				)
			}
		})
	})
}
