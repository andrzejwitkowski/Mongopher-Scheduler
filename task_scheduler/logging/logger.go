package logging

import (
	"sync"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap"
)


var (
	logger *zap.Logger
	once   sync.Once
)

func GetLogger() *zap.Logger {
	once.Do(func() {
		logger, _ = zap.NewProduction()
		setupLogger(logger)
	})
	return logger
}

func setupLogger(logger *zap.Logger) {
	// Add goroutine ID to log entries
	core := &goroutineIDCore{logger.Core()}
	// Set up logging
	logger.WithOptions(
		zap.AddCaller(),
		zap.AddStacktrace(zap.ErrorLevel),
		zap.Development(),
		zap.WithClock(zapcore.DefaultClock),
		zap.WrapCore(func(c zapcore.Core) zapcore.Core {
            return core
        }),
	)
}

