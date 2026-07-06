package observability

import "strings"

// Execution references (GH #78): every execution endpoint addresses a run by
// a single wire token — the `execution_arn` field of /pipeline/status rows,
// and the `arn=` query param of the /pipeline/execution detail/states/logs
// endpoints. FormatExecRef / SplitExecRef below are the ONE encode/decode
// pair for that token; both the status listing and the read endpoints go
// through them so the two halves of the flow can always exchange refs.
//
// Three shapes, one per run-address class:
//
//   - SFN execution ARN (fully-cloud run): passed through unchanged. The ARN
//     is self-contained, and wrapping it would break the colon-split in
//     StateMachineNameFromExecutionARN.
//   - non-ARN run with a pipeline dir: "local:<dir>#<runID>". `#` (vs `:`)
//     separates dir from runID so dirs containing colons round-trip; runID
//     may be empty, meaning "the pipeline's latest run".
//   - non-ARN run without a dir: the bare runID (e.g. an ADR-024 cloud-local
//     `local-<uuid>` — the warehouse `_progress` tree is keyed by run id
//     alone, so no dir is needed to resolve it).

// FormatExecRef encodes a (dir, runID) pair into the single exec-ref token.
func FormatExecRef(dir, runID string) string {
	if StateMachineNameFromExecutionARN(runID) != "" {
		return runID
	}
	if dir == "" {
		return runID
	}
	return "local:" + dir + "#" + runID
}

// SplitExecRef is the inverse of FormatExecRef. It additionally accepts the
// legacy "<dir>:<runID>" composite (the pre-GH-78 encoding the states/logs
// endpoints used) so refs minted by an older binary still decode.
//
//   - SFN execution ARN   → ("", arn)
//   - "local:<dir>#<id>"  → (dir, id)
//   - "<dir>:<id>"        → (dir, id)   [legacy; splits on the LAST colon so
//     absolute paths and Windows drive letters stay in the dir half]
//   - anything else       → ("", ref)   [a bare run id]
func SplitExecRef(ref string) (dir, runID string) {
	if ref == "" {
		return "", ""
	}
	if StateMachineNameFromExecutionARN(ref) != "" {
		return "", ref
	}
	if rest, ok := strings.CutPrefix(ref, "local:"); ok {
		// Split on the LAST '#': run ids never contain '#', dirs could.
		if i := strings.LastIndex(rest, "#"); i >= 0 {
			return rest[:i], rest[i+1:]
		}
		return rest, ""
	}
	if i := strings.LastIndex(ref, ":"); i >= 0 {
		return ref[:i], ref[i+1:]
	}
	return "", ref
}
