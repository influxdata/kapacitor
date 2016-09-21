package loggingtest

import (
	"io"
	"log"
	"os"

	"github.com/influxdata/kapacitor/services/logging"
	"github.com/influxdata/wlog"
)

type TestLogService struct{}

func New() TestLogService {
	return TestLogService{}
}

func (l TestLogService) NewLogger(prefix string, flag int) *log.Logger {
	return wlog.New(os.Stderr, prefix, flag)
}
func (l TestLogService) NewRawLogger(prefix string, flag int) *log.Logger {
	return log.New(os.Stderr, prefix, flag)
}

func (l TestLogService) NewStaticLevelLogger(prefix string, flag int, level logging.Level) *log.Logger {
	return log.New(wlog.NewStaticLevelWriter(os.Stderr, wlog.Level(level)), prefix, flag)
}

func (l TestLogService) NewStaticLevelWriter(level logging.Level) io.Writer {
	return wlog.NewStaticLevelWriter(os.Stderr, wlog.Level(level))
}
