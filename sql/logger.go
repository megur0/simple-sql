package sql

import (
	"context"
	"log"
)

var (
	l Logger = &defaultLogger{}
)

func SetLogger(lg Logger) {
	l = lg
}

type Logger interface {
	Info(c context.Context, message string, args ...any)
	Debug(c context.Context, message string, args ...any)
	Warn(c context.Context, message string, args ...any)
	Error(c context.Context, message string, args ...any)
}

type defaultLogger struct{}

func (l *defaultLogger) Info(c context.Context, message string, args ...any) {
	log.Print(message)
}

func (l *defaultLogger) Debug(c context.Context, message string, args ...any) {
	log.Print("[DEBUG]"+message, args)
}

func (l *defaultLogger) Warn(c context.Context, message string, args ...any) {
	log.Print("[Warn]"+message, args)
}

func (l *defaultLogger) Error(c context.Context, message string, args ...any) {
	log.Print("[Error]"+message, args)
}
