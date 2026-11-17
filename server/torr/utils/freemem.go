package utils

import (
	"runtime"
	"runtime/debug"
	"sync"
	"time"
)

func FreeOSMem() {
	debug.FreeOSMemory()
}

func FreeOSMemGC() {
	runtime.GC()
	debug.FreeOSMemory()
}

const gcInterval = 4 * time.Second

var (
	gcMu   sync.Mutex
	lastGC time.Time
)

// FreeOSMemGCThrottled asynchronously runs at most one
// GC and scavenge operation per gcInterval.
func FreeOSMemGCThrottled() {
	if !gcMu.TryLock() {
		return
	}

	if time.Since(lastGC) < gcInterval {
		gcMu.Unlock()
		return
	}

	go func() {
		defer gcMu.Unlock()
		lastGC = time.Now()
		FreeOSMem()
	}()
}
