// Package errs holds sentinel errors shared across internal/service,
// internal/api, internal/pipelinestatus, and internal/observability so
// each package can answer them with errors.Is without depending on the
// others. The "service can't be imported from pipelinestatus" rule that
// motivated bridge files in cli/ui.go (errInFlightBridge) is the actual
// problem these sentinels exist to fix (C10 from the 2026-05-24 review).
package errs

import "errors"

// ErrRunInFlight is the sentinel returned when an asynchronous pipeline
// run is already executing. service.StartRun returns it; the
// pipelinestatus HTTP layer maps it to 409.
var ErrRunInFlight = errors.New("a run is already in progress for this pipeline")

// ErrLocalNotImplemented signals a LocalProvider method that needs the
// runner / Spark catalog and hasn't been wired yet — handlers translate
// to 501. observability.LocalProvider returns it; pipelinestatus +
// dataquery handlers map.
var ErrLocalNotImplemented = errors.New("local provider: not yet implemented")

// Service-layer status sentinels — wrap with %w when constructing
// service errors so the api layer can pick a status without
// string-matching on err.Error(). Mapped to HTTP statuses by
// httputil.WriteServiceError.
var (
	// ErrNotFound — the addressed resource (pipeline, node, source,
	// credential, dashboard) does not exist. 404.
	ErrNotFound = errors.New("not found")
	// ErrInvalidInput — caller-fixable validation failure (bad ref
	// shape, invalid identifier, type=source on /pipeline/nodes). 400.
	ErrInvalidInput = errors.New("invalid input")
	// ErrConflict — the operation would violate an invariant (slug
	// already taken, schema already owned, duplicate registration). 409.
	ErrConflict = errors.New("conflict")
	// ErrUpstream — a backing service (Lambda, Athena, Glue) returned
	// a real error. 502.
	ErrUpstream = errors.New("upstream error")
)
