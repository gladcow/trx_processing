package logger

import (
	"go.uber.org/zap"
)

var Log = zap.NewNop()

func Initialize() error {
	var err error
	Log, err = zap.NewDevelopment()
	return err
}

func Info(msg string) {
	Log.Sugar().Info(msg)
}

func Infof(format string, args ...any) {
	Log.Sugar().Infof(format, args...)
}

func Warn(msg string) {
	Log.Sugar().Warn(msg)
}

func Warnf(format string, args ...any) {
	Log.Sugar().Warnf(format, args...)
}

func Error(msg string) {
	Log.Sugar().Error(msg)
}

func Errorf(format string, args ...any) {
	Log.Sugar().Errorf(format, args...)
}

func Fatalf(format string, args ...any) {
	Log.Sugar().Fatalf(format, args...)
}
