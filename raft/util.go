package raft

import (
	"bytes"
	"log"
	"runtime"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func SameByteSlice(s1, s2 []byte) bool {
	return bytes.Compare(s1, s2) == 0
}
func SameEntrySlice(s1 []LogEntry, s2 []LogEntry) bool {
	if len(s1) != len(s2) {
		return false
	}
	for i := 0; i < len(s1); i++ {
		if SameEntry(s1[i], s2[i]) == false {
			return false
		}
	}
	return true
}

func SameEntry(e1, e2 LogEntry) bool {
	return e1.Index == e2.Index && e1.Term == e2.Term && SameByteSlice(e1.Data, e2.Data)
}

func SameSnapshot(s1, s2 Snapshot) bool {
	return s1.Metadata == s2.Metadata && SameByteSlice(s1.Data, s2.Data)
}

func PrintMemoryStatus(prefix string, l *log.Logger) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	l.Printf("\n%s: Alloc = %vMB, Heap Inuse = %vMB, Sys = %vMB, NumGC = %v, NumGoR = %v\n", prefix,
		float64(m.Alloc)/(1024*1024), float64(m.HeapInuse)/(1024*1024), float64(m.Sys)/(1024*1024), m.NumGC, runtime.NumGoroutine())
}
