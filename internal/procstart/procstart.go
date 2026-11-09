// Package procstart records the wall-clock moment the process started.
//
// It has no imports beyond time, so Go initializes it before almost every
// other package; cmd/clavesa imports it first so At is as close to exec as
// a Go program can observe. The run-timing record uses it for the
// `startup` phase: process start to the entry of the run service call.
package procstart

import "time"

// At is the time this package was initialized (process start, give or take
// the runtime's own boot).
var At = time.Now()
