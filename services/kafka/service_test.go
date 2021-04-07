package kafka_test

import (
	"os"
	"testing"

	"github.com/influxdata/kapacitor/services/diagnostic"
	"github.com/influxdata/kapacitor/services/kafka"
	"github.com/influxdata/kapacitor/services/kafka/kafkatest"
	"github.com/stretchr/testify/require"
)

func TestWriter_UpdateConfig(t *testing.T) {
	ts, err := kafkatest.NewServer()
	require.NoError(t, err)
	defer ts.Close()

	c := kafka.NewConfig()
	c.Enabled = true
	c.Brokers = []string{ts.Addr.String()}

	cluster := kafka.NewCluster(c)
	defer cluster.Close()
	diag := diagnostic.NewService(diagnostic.NewConfig(), os.Stderr, os.Stdin)
	require.NoError(t, diag.Open())
	defer diag.Close()

	// Write a message to generate a writer.
	require.NoError(t, cluster.WriteMessage(diag.NewKafkaHandler(),
		kafka.WriteTarget{
			Topic:              "testTopic",
			PartitionById:      false,
			PartitionAlgorithm: "",
		}, []byte{1, 2, 3, 4}, []byte{1, 2, 3, 4}))

	// Update the config in a way that requires shutting down all active writers.
	c.UseSSL = true
	require.NoError(t, cluster.Update(c))
}
