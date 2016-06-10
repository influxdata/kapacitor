package uuid_test

import (
	"github.com/twinj/uuid"
	_ "github.com/twinj/uuid/savers"
	"testing"
	_ "time"
)

func BenchmarkNewV1(b *testing.B) {
	//uuid.RegisterSaver(&savers.FileSystemSaver{Report:true, Duration: time.Second*1})
	//b.ResetTimer()
	for i := 0; i < b.N; i++ {
		uuid.NewV1()
	}
	b.StopTimer()
	b.ReportAllocs()
}

func BenchmarkNewV2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		uuid.NewV2(uuid.DomainGroup)
	}
	b.StopTimer()
	b.ReportAllocs()
}

func BenchmarkNewV3(b *testing.B) {
	for i := 0; i < b.N; i++ {
		uuid.NewV3(uuid.NamespaceDNS, uuid.Name("www.example.com"))
	}
	b.StopTimer()
	b.ReportAllocs()
}

func BenchmarkNewV4(b *testing.B) {
	for i := 0; i < b.N; i++ {
		uuid.NewV4()
	}
	b.StopTimer()
	b.ReportAllocs()
}

func BenchmarkNewV5(b *testing.B) {
	for i := 0; i < b.N; i++ {
		uuid.NewV5(uuid.NamespaceDNS, uuid.Name("www.example.com"))
	}
	b.StopTimer()
	b.ReportAllocs()
}

func BenchmarkParse(b *testing.B) {
	s := "f3593cff-ee92-40df-4086-87825b523f13"
	for i := 0; i < b.N; i++ {
		uuid.Parse(s)
	}
	b.StopTimer()
	b.ReportAllocs()
}
