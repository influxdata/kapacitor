package kapacitor

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/influxdata/kapacitor/pipeline"
	"github.com/uber-go/zap"
)

type LogNode struct {
	node
	level  zap.Level
	l      zap.Logger
	prefix string
}

// Create a new  LogNode which logs all data it receives
func newLogNode(et *ExecutingTask, n *pipeline.LogNode, l zap.Logger) (*LogNode, error) {
	var level zap.Level
	switch strings.ToUpper(n.Level) {
	case "DEBUG":
		level = zap.DebugLevel
	case "INFO":
		level = zap.InfoLevel
	case "WARN":
		level = zap.WarnLevel
	case "ERROR":
		level = zap.ErrorLevel
	}
	nn := &LogNode{
		node:   node{Node: n, et: et, logger: l},
		level:  level,
		l:      l.With(zap.String("prefix", n.Prefix)),
		prefix: n.Prefix + " ",
	}
	nn.node.runF = nn.runLog
	return nn, nil
}

func (s *LogNode) runLog([]byte) error {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	switch s.Wants() {
	case pipeline.StreamEdge:
		for p, ok := s.ins[0].NextPoint(); ok; p, ok = s.ins[0].NextPoint() {
			buf.Reset()
			buf.WriteString(s.prefix)
			if err := enc.Encode(p); err != nil {
				s.logger.Error(err.Error())
				continue
			}
			// Drop newline
			buf.Truncate(buf.Len() - 1)
			s.l.Log(s.level, buf.String())
			for _, child := range s.outs {
				err := child.CollectPoint(p)
				if err != nil {
					return err
				}
			}
		}
	case pipeline.BatchEdge:
		for b, ok := s.ins[0].NextBatch(); ok; b, ok = s.ins[0].NextBatch() {
			buf.Reset()
			buf.WriteString(s.prefix)
			if err := enc.Encode(b); err != nil {
				s.logger.Error(err.Error())
				continue
			}
			// Drop newline
			buf.Truncate(buf.Len() - 1)
			s.l.Log(s.level, buf.String())
			for _, child := range s.outs {
				err := child.CollectBatch(b)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}
