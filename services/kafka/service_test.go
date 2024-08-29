package kafka_test

import (
	"log"
	"os"
	"testing"

	"github.com/IBM/sarama"
	"github.com/influxdata/kapacitor/services/diagnostic"
	"github.com/influxdata/kapacitor/services/kafka"
	"github.com/influxdata/kapacitor/services/kafka/kafkatest"
	"github.com/stretchr/testify/require"
)

func TestWriter_UpdateConfig(t *testing.T) {
	sarama.DebugLogger = log.New(os.Stderr, "[kapacitor kafka] ", log.LstdFlags)
	ts, err := kafkatest.NewServer()
	require.NoError(t, err)

	c := kafka.NewConfig()
	c.Enabled = true
	c.Brokers = []string{ts.Addr.String()}
	cluster := kafka.NewCluster(c)
	diag := diagnostic.NewService(diagnostic.NewConfig(), os.Stderr, os.Stdin)
	require.NoError(t, diag.Open())

	// Write a message to generate a writer.
	require.NoError(t, cluster.WriteMessage(diag.NewKafkaHandler(),
		kafka.WriteTarget{
			Topic:              "testTopic",
			PartitionById:      false,
			PartitionAlgorithm: "",
		}, []byte{1, 2, 3, 4}, []byte{1, 2, 3, 4}))

	c.UseSSL = true
	require.NoError(t, cluster.Update(c))
	require.NoError(t, diag.Close())
	cluster.Close()
	ts.Close()
	t.Log("test done")
}
