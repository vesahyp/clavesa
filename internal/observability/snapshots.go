package observability

import (
	"strconv"

	"github.com/vesahyp/clavesa/internal/delta"
)

// snapshotsResultFromCommits projects a table's Delta commit history
// (newest-first, as delta.ReadTableState / ReadTableStateFromPath return
// it) into the SnapshotsResult wire shape: LatestRecordCount, Total, the
// limit-truncated SnapshotInfo list, and the Truncated flag. Shared by
// CloudProvider.Snapshots and LocalProvider.Snapshots so the two providers
// cannot drift on the catalog row count — the number ADR-014 most visibly
// binds. limit <= 0 means "no truncation".
//
// rowCount is delta.TableState.RowCount — the exact current row count
// derived from Delta snapshot state (checkpoint stats + post-checkpoint
// replay). When present it wins outright; it is the GH #66 fix, immune to
// the commit-window and log-retention truncation the fold below suffers.
// When nil, the newest commit's writer-stamped TotalRecords is used, and
// failing that the per-commit fold.
//
// The fold walks commits oldest-first. Three commit shapes matter for
// table-state row count:
//   - Replaces=true (CTAS, CREATE OR REPLACE, WRITE Overwrite): reset the
//     running total to this commit's net delta.
//   - MERGE: net delta is (added - updated - deleted). The delta log_reader
//     folds inserts+updates into AddedRecords for the timeline display;
//     subtract UpdatedRecords here because updates don't change row count.
//   - Otherwise (APPEND, DELETE): net delta is (added - deleted).
//
// The fold is exact only when the window anchors — it reaches the table's
// creation (version 0) or contains a Replaces commit. Otherwise it starts
// from an arbitrary mid-history zero and the result is flagged
// LatestRecordCountApproximate so the UI can say so instead of asserting a
// wrong number.
func snapshotsResultFromCommits(commits []delta.Commit, limit int, rowCount *int64) *SnapshotsResult {
	// Total: the number of commits ever made to the table. Delta versions
	// are contiguous from 0, so newest version + 1 is exact even when log
	// retention or the read window hides older commits — len(commits)
	// would understate both ways (GH #66).
	totalCommits := 0
	if len(commits) > 0 {
		totalCommits = int(commits[0].Version) + 1
	}

	var foldCount *int64
	anchored := false
	if len(commits) > 0 {
		// Window reaching version 0 sees the whole history; a Replaces
		// commit resets the fold to a known-complete state either way.
		if commits[len(commits)-1].Version == 0 {
			anchored = true
		}
		var total int64
		for i := len(commits) - 1; i >= 0; i-- {
			ci := commits[i]
			added := int64(0)
			if ci.AddedRecords != nil {
				added = *ci.AddedRecords
			}
			updated := int64(0)
			if ci.UpdatedRecords != nil {
				updated = *ci.UpdatedRecords
			}
			deleted := int64(0)
			if ci.DeletedRecords != nil {
				deleted = *ci.DeletedRecords
			}
			netDelta := added - updated - deleted
			if ci.Replaces {
				total = netDelta
				anchored = true
			} else {
				total += netDelta
			}
		}
		if total < 0 {
			total = 0
		}
		foldCount = &total
	}

	if limit <= 0 {
		limit = len(commits)
	}
	truncated := false
	if len(commits) > limit {
		commits = commits[:limit]
		truncated = true
	}

	out := make([]SnapshotInfo, 0, len(commits))
	for _, ci := range commits {
		trigger, runID := extractProvenance(ci.UserMetadata)
		info := SnapshotInfo{
			SnapshotID:     strconv.FormatInt(ci.Version, 10),
			CommittedAt:    formatMillis(ci.TimestampMs),
			Operation:      ci.Operation,
			AddedRecords:   ci.AddedRecords,
			DeletedRecords: ci.DeletedRecords,
			TotalRecords:   ci.TotalRecords,
			Trigger:        trigger,
			WriterRunID:    runID,
		}
		// Delta doesn't carry a `parent_id` per commit the way Iceberg
		// did — versions are strictly monotonic, so the previous
		// version's id is "this one minus 1" for v > 0. Surface that
		// for UI back-compat; the field is optional in the JSON shape
		// and the UI uses it for nothing more than rendering "v3 ← v2".
		if ci.Version > 0 {
			info.ParentID = strconv.FormatInt(ci.Version-1, 10)
		}
		out = append(out, info)
	}

	res := &SnapshotsResult{Snapshots: out, Truncated: truncated, Total: totalCommits}
	switch {
	case rowCount != nil:
		v := *rowCount
		res.LatestRecordCount = &v
	case len(out) > 0 && out[0].TotalRecords != nil:
		// Writer-stamped total on the newest commit — exact when present.
		v := *out[0].TotalRecords
		res.LatestRecordCount = &v
	case foldCount != nil:
		res.LatestRecordCount = foldCount
		res.LatestRecordCountApproximate = !anchored
	}
	return res
}
