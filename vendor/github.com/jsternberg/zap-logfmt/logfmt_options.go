package zaplogfmt

import (
	"time"

	"go.uber.org/zap"
)

type LogfmtOption interface {
	apply(*logfmtEncoder)
}

type logfmtOptionFunc func(*logfmtEncoder)

func (opt logfmtOptionFunc) apply(enc *logfmtEncoder) {
	opt(enc)
}

type MessageFormatter func(string) zap.Field

func (mf MessageFormatter) apply(enc *logfmtEncoder) {
	enc.messageF = mf
}

func MessageKey(key string) MessageFormatter {
	return MessageFormatter(func(msg string) zap.Field {
		return zap.String(key, msg)
	})
}

type TimeFormatter func(time.Time) zap.Field

func (tf TimeFormatter) apply(enc *logfmtEncoder) {
	enc.timeF = tf
}

func EpochFormatter(key string) TimeFormatter {
	return TimeFormatter(func(t time.Time) zap.Field {
		return zap.Time(key, t)
	})
}

func RFC3339Formatter(key string) TimeFormatter {
	return TimeFormatter(func(t time.Time) zap.Field {
		return zap.String(key, t.Format(time.RFC3339))
	})
}

func NoTime() TimeFormatter {
	return TimeFormatter(func(time.Time) zap.Field {
		return zap.Skip()
	})
}

type LevelFormatter func(zap.Level) zap.Field

func (lf LevelFormatter) apply(enc *logfmtEncoder) {
	enc.levelF = lf
}

func LevelString(key string) LevelFormatter {
	return LevelFormatter(func(l zap.Level) zap.Field {
		return zap.String(key, l.String())
	})
}
