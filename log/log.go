package log

import (
	"log"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	DebugLevel = iota
	InfoLevel
	ErrorLevel
)

const (
	levelDebug = "debug"
	levelInfo  = "info"
	levelError = "error"
)

var (
	logLevel uint32

	logger log.Logger // universe logger

	once        sync.Once
	initialized uint32
)

func Setup(c LogConf) error {
	once.Do(func() {
		logger = *log.Default() // todo mo options
		atomic.StoreUint32(&initialized, 1)
		setupLogLevel(c.Level)
	})
	return nil
}

func setupLogLevel(l string) {
	switch {
	case strings.EqualFold(l, levelDebug):
		SetLevel(DebugLevel)
	case strings.EqualFold(l, levelInfo):
		SetLevel(InfoLevel)
	case strings.EqualFold(l, levelError):
		SetLevel(ErrorLevel)
	}
}

func SetLevel(level uint32) {
	atomic.StoreUint32(&logLevel, level)
}

func shallLog(level uint32) bool {
	return atomic.LoadUint32(&logLevel) <= level
}

func Debugf(format string, v ...interface{}) {
	if shallLog(DebugLevel) {
		logger.Printf(format, v...)
	}
}

func Infof(format string, v ...interface{}) {
	if shallLog(InfoLevel) {
		logger.Printf(format, v...)
	}
}

func Errorf(format string, v ...interface{}) {
	if shallLog(ErrorLevel) {
		logger.Printf(format, v...)
	}
}
