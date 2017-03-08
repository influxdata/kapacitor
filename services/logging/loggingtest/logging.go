package loggingtest

import (
	"io"
	"log"
	"os"

	"github.com/influxdata/kapacitor/services/logging"
	"github.com/influxdata/wlog"
)

func init() {
	wlog.SetLevel(wlog.DEBUG)
}

type TestLogService struct {
	prefix string
}

func New() TestLogService {
	return NewWithPrefix("")
}
func NewWithPrefix(prefix string) TestLogService {
	return TestLogService{
		prefix: prefix,
	}
}

func (l TestLogService) NewLogger(prefix string, flag int) *log.Logger {
	return wlog.New(os.Stderr, l.prefix+prefix, flag)
}
func (l TestLogService) NewRawLogger(prefix string, flag int) *log.Logger {
	return log.New(os.Stderr, l.prefix+prefix, flag)
}

func (l TestLogService) NewStaticLevelLogger(prefix string, flag int, level logging.Level) *log.Logger {
	return log.New(wlog.NewStaticLevelWriter(os.Stderr, wlog.Level(level)), l.prefix+prefix, flag)
}

func (l TestLogService) NewStaticLevelWriter(level logging.Level) io.Writer {
	return wlog.NewStaticLevelWriter(os.Stderr, wlog.Level(level))
}
