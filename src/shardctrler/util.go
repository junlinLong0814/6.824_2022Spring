package shardctrler

import (
	"fmt"
	"time"
)

// Debugging
const Log = 0

func TimeInfo() string {
	return "[" + time.Now().Format("2006-01-02 15:04:05.000") + "]"
}

func NoTimeLogInfo(format string, a ...interface{}) (n int, err error) {
	if Log > 0 {
		fmt.Printf(format, a...)
	}
	return
}

func LogInfo(format string, a ...interface{}) (n int, err error) {
	if Log > 0 {
		format = TimeInfo() + format
		fmt.Printf(format, a...)
	}
	return
}
