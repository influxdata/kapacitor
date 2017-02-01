package loggingtest

import (
	"io"
	"os"

	"go.uber.org/zap"
)

type TestLogService struct {
	root zap.Logger
}

func New() TestLogService {
	return TestLogService{
		root: zap.New(zap.NewTextEncoder(), zap.Output(os.Stderr), zap.DebugLevel),
	}
}

func (l TestLogService) Root() zap.Logger {
	return l.root
}
func (l TestLogService) Writer() io.Writer {
	return os.Stderr
}
func (l TestLogService) SetLevel(string) error {
	return nil
}
