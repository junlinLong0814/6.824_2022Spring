package raft

import (
	"bytes"
	"fmt"
	"log"
	"time"

	"6.824/labgob"
)

// Debugging
const Debug = 0
const Vote = 0
const MyDebug = 0
const Log = 0
const State = 0
const Snap = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func TimeInfo() string {
	return "[" + time.Now().Format("2006-01-02 15:04:05.000") + "]"
}

func StateInfo(format string, a ...interface{}) (n int, err error) {
	if State > 0 {
		format = TimeInfo() + format
		fmt.Printf(format, a...)
	}
	return
}

func SnapInfo(format string, a ...interface{}) (n int, err error) {
	if Snap > 0 {
		format = TimeInfo() + format
		fmt.Printf(format, a...)
	}
	return
}

func VoteInfo(format string, a ...interface{}) (n int, err error) {
	if Vote > 0 {
		fmt.Printf(format, a...)
	}
	return
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

func NoTimeDebug(format string, a ...interface{}) (n int, err error) {
	if MyDebug > 0 {
		fmt.Printf(format, a...)
	}
	return
}

func DeBugPrintf(format string, a ...interface{}) (n int, err error) {
	if MyDebug > 0 {
		format = TimeInfo() + format
		fmt.Printf(format, a...)
	}
	return
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func serialization(rf *Raft) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	return data
}
