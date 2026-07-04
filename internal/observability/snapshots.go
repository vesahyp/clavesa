package observability

import (
	"strconv"

	"github.com/vesahyp/clavesa/internal/delta"
)

// snapshotsResultFromCommits projects a table's Delta commit history
// (newest-first, as delta.ReadCurrent / ReadCurrentFromPath return it) into
// the SnapshotsResult wire shape: LatestRecordCount, Total, the
// limit-truncated SnapshotInfo list, and the Truncated flag. Shared by
// CloudProvider.Snapshots and LocalProvider.Snapshots so the two providers
// cannot drift on the catalog row count — the number ADR-014 most visibly
// binds. limit <= 0 means "no truncation".
//
// LatestRecordCount folds commits oldest-first. Three commit shapes matter
// for table-state row count:
//   - Replaces=true (CTAS, CREATE OR REPLACE, WRITE Overwrite): reset the
//     running total to this commit's net delta.
//   - MERGE: net delta is (added - updated - deleted). The delta log_reader
//     folds inserts+updates into AddedRecords for the timeline display;
//     subtract UpdatedRecords here because updates don't change row count.
//   - Otherwise (APPEND, DELETE): net delta is (added - deleted).
//
// KNOWN BUG (GH #66): delta.ReadCurrent caps the commit list at the newest
// 200 commits (and Delta log retention deletes older JSON commits anyway),
// so for a table without a Replaces commit inside the surviving window the
// fold starts from an arbitrary mid-history zero and understates the row
// count; Total is capped the same way. Deliberately preserved by this
// extraction — the #66 fix lands once, here.
func snapshotsResultFromCommits(commits []delta.Commit, limit int) *SnapshotsResult {
	// Full commit count before any limit truncation — the catalog shows
	// this as the real number of commits instead of "<limit>+".
	totalCommits := len(commits)

	var latestCount *int64
	if len(commits) > 0 {
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
			} else {
				total += netDelta
			}
		}
		if total < 0 {
			total = 0
		}
		latestCount = &total
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
	// Prefer the newest commit's own TotalRecords when the writer stamped
	// it; fall back to the folded running sum.
	if len(out) > 0 && out[0].TotalRecords != nil {
		v := *out[0].TotalRecords
		res.LatestRecordCount = &v
	} else if latestCount != nil {
		res.LatestRecordCount = latestCount
	}
	return res
}
