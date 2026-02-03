package logger

import (
	"log"
	"os"
)

var std = log.New(os.Stdout, "", log.LstdFlags|log.LUTC|log.Lshortfile)

func Infof(format string, args ...any) {
	std.Printf("INFO: "+format, args...)
}

func Errorf(format string, args ...any) {
	std.Printf("ERROR: "+format, args...)
}

func Fatalf(format string, args ...any) {
	std.Fatalf("FATAL: "+format, args...)
}
