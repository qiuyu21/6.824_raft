package raft

import (
	"log"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}else{
		return b
	}
}


func max(a, b uint64) uint64 {
	if a > b {
		return a
	}else{
		return b
	}
}


func backoff(base time.Duration, round, limit uint64) time.Duration {
	power := min(round, limit)
	for power > 2 {
		base *= 2
		power-- 
	}
	return base
}