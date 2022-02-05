package kafka

import (
	"errors"
	"hash/crc32"
	"math/rand"
	"time"

	"github.com/Shopify/sarama"
)

var ErrNonPositivePartitions = errors.New("number of partitions must be positive")

func newMurmur2(topic string) sarama.Partitioner {
	return (*murmur2)(rand.New(rand.NewSource(time.Now().UTC().UnixNano())))
}

// murmur2 is a partitioner that uses murmur2 it is designed to be compatible with older versions of kapacitor
// We can also have it be a rand.Rand because with the async producer,
// it is never called from multiple goroutines.
type murmur2 rand.Rand

// Partition here is based on the murmur2 balance function in github.com/segmentio/kafka-go to not break compatibility
// with earlier versions.
func (b *murmur2) Partition(msg *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	if numPartitions == 0 {
		return 0, ErrNonPositivePartitions
	}

	// NOTE: the murmur2 balancers in java and librdkafka treat a nil key as
	//       non-existent while treating an empty slice as a defined value.
	if msg.Key == nil {
		return (*rand.Rand)(b).Int31n(numPartitions), nil
	}
	key, err := msg.Key.Encode()
	if err != nil {
		return 0, err
	}
	return int32((murmur2Hash(key) & 0x7fffffff) % uint32(numPartitions)), nil
}

func (*murmur2) RequiresConsistency() bool { return true }

// murmur2 is taken from github.com/segmentio/kafka-go
// at https://github.com/segmentio/kafka-go/blob/4da3b721ca38db775a5024089cdc4ba14f84e698/balancer.go#L251
// We copy here because it was in a private method.
// It is a go port of the Java library's murmur2 function.
// https://github.com/apache/kafka/blob/1.0/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L353
func murmur2Hash(data []byte) uint32 {
	length := len(data)
	const (
		seed uint32 = 0x9747b28c
		// 'm' and 'r' are mixing constants generated offline.
		// They're not really 'magic', they just happen to work well.
		m = 0x5bd1e995
		r = 24
	)

	// Initialize the hash to a  predetermined "random" value.  This is the standard way for murmur2
	h := seed ^ uint32(length)
	length4 := length / 4

	for i := 0; i < length4; i++ {
		i4 := i * 4
		k := (uint32(data[i4+0]) & 0xff) + ((uint32(data[i4+1]) & 0xff) << 8) + ((uint32(data[i4+2]) & 0xff) << 16) + ((uint32(data[i4+3]) & 0xff) << 24)
		k *= m
		k ^= k >> r
		k *= m
		h *= m
		h ^= k
	}

	// Handle the last few bytes of the input array
	extra := length % 4
	if extra >= 3 {
		h ^= (uint32(data[(length & ^3)+2]) & 0xff) << 16
	}
	if extra >= 2 {
		h ^= (uint32(data[(length & ^3)+1]) & 0xff) << 8
	}
	if extra >= 1 {
		h ^= uint32(data[length & ^3]) & 0xff
		h *= m
	}

	h ^= h >> 13
	h *= m
	h ^= h >> 15

	return h
}

func newCRCPartitioner(topic string) sarama.Partitioner {
	return (*crcpartitioner)(rand.New(rand.NewSource(time.Now().UTC().UnixNano())))
}

// crcpartitioner is a partitioner for crc32 we use it instead of just using sarama.NewCustomHashPartitioner(crc32.NewIEEE),
// to maintain compatibility with previous kapacitor versions, because the way that sarama distributes the result of
// the hash causes us to lose compatibility.  We can also have it be a rand.Rand because with the async producer,
// it is never called from multiple goroutines.
type crcpartitioner rand.Rand

// Partition takes a message and partition count and chooses a partition
// we made a custom partitioner, because
func (b *crcpartitioner) Partition(msg *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	if numPartitions == 0 {
		return 0, ErrNonPositivePartitions
	}
	// NOTE: the crc32 balancers in librdkafka don't differentiate between nil
	//       and empty keys.  both cases are treated as unset.
	if msg.Key == nil || msg.Key.Length() == 0 {
		return (*rand.Rand)(b).Int31n(numPartitions), nil
	}
	key, err := msg.Key.Encode()
	if err != nil {
		return 0, err
	}
	return int32((crc32.ChecksumIEEE(key) % uint32(numPartitions)) & 0x7fffffff), nil
}

// RequiresConsistency indicates to the user of the partitioner whether the
// mapping of key->partition is consistent or not. Specifically, if a
// partitioner requires consistency then it must be allowed to choose from all
// partitions (even ones known to be unavailable), and its choice must be
// respected by the caller. The obvious example is the HashPartitioner.
func (*crcpartitioner) RequiresConsistency() bool {
	return true
}
