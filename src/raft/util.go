package raft

import "log"

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

type matchIndexSlice []uint64
func (s matchIndexSlice) Len() int           { return len(s) }
func (s matchIndexSlice) Less(i, j int) bool { return s[i] < s[j] }
func (s matchIndexSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
