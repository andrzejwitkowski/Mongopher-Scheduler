package logging

import (
	"runtime"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func GetGoroutineID() string {
    buf := make([]byte, 64)
    buf = buf[:runtime.Stack(buf, false)]
    // Extract the goroutine ID (the number after "goroutine " in the stack trace)
    stack := string(buf)
    stack = strings.TrimPrefix(stack, "goroutine ")
    stack = strings.TrimSuffix(stack, " [running]:\n")
    return stack
}

type goroutineIDCore struct {
    zapcore.Core
}

func (c *goroutineIDCore) Check(entry zapcore.Entry, checkedEntry *zapcore.CheckedEntry) *zapcore.CheckedEntry {
    if c.Enabled(entry.Level) {
        return checkedEntry.AddCore(entry, c)
    }
    return checkedEntry
}

func (c *goroutineIDCore) With(fields []zapcore.Field) zapcore.Core {
    return &goroutineIDCore{Core: c.Core.With(fields)}
}

func (c *goroutineIDCore) Write(entry zapcore.Entry, fields []zapcore.Field) error {
    // Add the Goroutine ID to the log entry
    fields = append(fields, zap.String("goroutineID", GetGoroutineID()))
    return c.Core.Write(entry, fields)
}
