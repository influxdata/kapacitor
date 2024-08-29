package kafka

import (
	"reflect"
	"testing"

	"github.com/IBM/sarama"
)

// This tests the nil case of the partitioners to ensure they are randomish.  We aren't testing for cryptografphic
// randomness, just that it isn't reverting back to standard empty string hashing behavior
func TestPartitionRandomness(t *testing.T) {
	partitioners := []sarama.Partitioner{
		newCRCPartitioner("x"),
		newMurmur2("x"),
	}
	for _, tt := range partitioners {
		msg := sarama.ProducerMessage{Key: nil}
		t.Run(reflect.TypeOf(tt).Name(), func(t *testing.T) {
			nSum := 0
			for i := 0; i < 1000; i++ {
				n, err := tt.Partition(&msg, 10)
				if err != nil {
					t.FailNow()
				}
				nSum += int(n)
			}
			if float64(nSum)/1000.0 >= 5 || float64(nSum)/1000 <= 4 {
				t.Errorf("expected partitioner to be more random, the average of 1000 trials from 0-9 was %f", float64(nSum)/1000)
			}
		})
	}
}

func Test_HashPartitioners(t *testing.T) {
	tests := []struct {
		key           string
		numPartitions int32
		want          int32
		wantErr       bool
		partitioner   sarama.Partitioner
	}{
		{key: "hello", numPartitions: 1000, want: 870, partitioner: newCRCPartitioner("a")},
		{key: "hello", numPartitions: 0, want: 0, wantErr: true, partitioner: newCRCPartitioner("a")},
		{key: "hello2", numPartitions: 1000, want: 502, partitioner: newCRCPartitioner("a")},
		{key: "hello3", numPartitions: 1000, want: 592, partitioner: newCRCPartitioner("a")},
		{key: "hello4", numPartitions: 1000, want: 787, partitioner: newCRCPartitioner("a")},
		{key: "hello5", numPartitions: 1000, want: 413, partitioner: newCRCPartitioner("a")},
		{key: "hello5", numPartitions: 1, want: 0, partitioner: newCRCPartitioner("a")},

		{key: "hello", numPartitions: 1000, want: 229, partitioner: newMurmur2("a")},
		{key: "hello", numPartitions: 0, want: 0, wantErr: true, partitioner: newMurmur2("a")},
		{key: "hello2", numPartitions: 1000, want: 907, partitioner: newMurmur2("a")},
		{key: "hello3", numPartitions: 1000, want: 759, partitioner: newMurmur2("a")},
		{key: "hello4", numPartitions: 1000, want: 299, partitioner: newMurmur2("a")},
		{key: "hello5", numPartitions: 1000, want: 841, partitioner: newMurmur2("a")},
		{key: "", numPartitions: 1000, want: 681, partitioner: newMurmur2("a")},
		{key: "hello5", numPartitions: 1, want: 0, partitioner: newMurmur2("a")},
	}
	for _, tt := range tests {
		t.Run(reflect.TypeOf(tt.partitioner).Name()+"/"+tt.key, func(t *testing.T) {
			msg := sarama.ProducerMessage{Key: sarama.ByteEncoder(tt.key)}
			got, err := tt.partitioner.Partition(&msg, tt.numPartitions)
			if (err != nil) != tt.wantErr {
				t.Errorf("Partition() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Partition() got = %v, want %v", got, tt.want)
			}
			if tt.partitioner.RequiresConsistency() != true {
				t.Errorf("Hash partitioners should always return true")
			}
		})
	}
}

func Test_murmur2Hash(t *testing.T) {
	tests := []struct {
		data string
		want int32 // this has to be int because the java library uses signed ints
	}{
		// The test set from the Java library for murmur2
		// https://github.com/apache/kafka/blob/8a1fcee86e42c8bd1f26309dde8748927109056e/clients/src/test/java/org/apache/kafka/common/utils/UtilsTest.java#L89
		{data: "21", want: -973932308},
		{data: "foobar", want: -790332482},
		{data: "a-little-bit-long-string", want: -985981536},
		{data: "a-little-bit-longer-string", want: -1486304829},
		{data: "lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8", want: -58897971},
		{data: "abc", want: 479470107},
	}
	for _, tt := range tests {
		t.Run(tt.data, func(t *testing.T) {
			uintWant := uint32(tt.want)
			if got := murmur2Hash([]byte(tt.data)); got != uintWant {
				t.Errorf("murmur2Hash() = %v, want %v", got, uintWant)
			}
		})
	}
}
