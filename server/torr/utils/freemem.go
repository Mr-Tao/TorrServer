package utils

import (
	"runtime"
	"runtime/debug"
	"sync/atomic"
	"time"
)

func FreeOSMem() {
	debug.FreeOSMemory()
}

func FreeOSMemGC() {
	runtime.GC()
	debug.FreeOSMemory()
}

var lastGC atomic.Int64

// FreeOSMemGCThrottled runs a real GC + scavenge at most once per 10 s, asynchronously.
func FreeOSMemGCThrottled() {
	now := time.Now().UnixNano()
	last := lastGC.Load()
	if now-last < 4e9 || !lastGC.CompareAndSwap(last, now) {
		return
	}
	go FreeOSMemGC()
}
