package observability

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Two full 64-hex container IDs, as `docker run -d` and `docker inspect
// -f {{.Id}}` print them, plus the 12-hex short form `docker ps -q`
// prints for the first one.
const (
	releaseTestID      = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	releaseTestOtherID = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
	releaseTestShortID = "0123456789ab"
)

// heldLease builds a lease in the given state for pid, expiring at exp.
func heldLease(state string, pid int, exp time.Time) leaseInfo {
	var l leaseInfo
	l.State = state
	l.Holder.PID = pid
	l.ExpiresAt = exp
	return l
}

// TestShouldReleaseMetastore pins the GH #99 release rules one at a time:
// the container must still be the one we created, no other container of
// the workspace may be running, and no other process may hold a live run
// lease. Everything else (our own lease, a released or expired lease, an
// empty world) releases.
func TestShouldReleaseMetastore(t *testing.T) {
	now := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	const pid = 4242
	cases := []struct {
		name       string
		recordedID string
		currentID  string
		others     []string
		leases     []leaseInfo
		want       bool
	}{
		{"empty everything releases", releaseTestID, releaseTestID, nil, nil, true},
		{"recorded vs current ID mismatch keeps (a UI recreated it)", releaseTestID, releaseTestOtherID, nil, nil, false},
		{"missing current ID keeps", releaseTestID, "", nil, nil, false},
		{"missing recorded ID keeps", "", releaseTestID, nil, nil, false},
		{"only our own metastore in the label listing releases", releaseTestID, releaseTestID, []string{releaseTestID}, nil, true},
		{"our own metastore listed in short form releases", releaseTestID, releaseTestID, []string{releaseTestShortID}, nil, true},
		{"another labeled container keeps", releaseTestID, releaseTestID, []string{releaseTestID, releaseTestOtherID}, nil, false},
		{"foreign held unexpired lease keeps", releaseTestID, releaseTestID, nil,
			[]leaseInfo{heldLease("held", pid+1, now.Add(time.Hour))}, false},
		{"own-PID held lease releases", releaseTestID, releaseTestID, nil,
			[]leaseInfo{heldLease("held", pid, now.Add(time.Hour))}, true},
		{"foreign released lease releases", releaseTestID, releaseTestID, nil,
			[]leaseInfo{heldLease("released", pid+1, now.Add(time.Hour))}, true},
		{"foreign expired lease releases", releaseTestID, releaseTestID, nil,
			[]leaseInfo{heldLease("held", pid+1, now.Add(-time.Minute))}, true},
		{"one live foreign lease among harmless ones keeps", releaseTestID, releaseTestID, nil,
			[]leaseInfo{
				heldLease("released", pid+1, now.Add(time.Hour)),
				heldLease("held", pid, now.Add(time.Hour)),
				heldLease("held", pid+2, now.Add(time.Minute)),
			}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := shouldReleaseMetastore(tc.recordedID, tc.currentID, tc.others, tc.leases, now, pid)
			if got != tc.want {
				t.Errorf("shouldReleaseMetastore = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestSameContainerID checks full and short docker IDs match each other in
// either order, and that nothing shorter than the 12-hex short form or
// empty ever matches (an empty ID must never look like "ours").
func TestSameContainerID(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{releaseTestID, releaseTestID, true},
		{releaseTestID, releaseTestShortID, true},
		{releaseTestShortID, releaseTestID, true},
		{releaseTestID, releaseTestOtherID, false},
		{releaseTestID, "", false},
		{"", "", false},
		{"0123456789a", releaseTestID, false},
	}
	for _, tc := range cases {
		if got := sameContainerID(tc.a, tc.b); got != tc.want {
			t.Errorf("sameContainerID(%q, %q) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}

// TestReadRunLeases parses the file backend's `<warehouse>/_locks/
// <pipeline>.run.json` documents as internal/runlock writes them, ignores
// files that are not leases, and reports a missing lock dir as no leases.
func TestReadRunLeases(t *testing.T) {
	warehouse := t.TempDir()
	if leases, err := readRunLeases(warehouse); err != nil || len(leases) != 0 {
		t.Fatalf("missing _locks dir: leases=%v err=%v, want none and nil", leases, err)
	}

	locks := filepath.Join(warehouse, "_locks")
	if err := os.MkdirAll(locks, 0o755); err != nil {
		t.Fatal(err)
	}
	doc := `{"holder":{"run_id":"r1","compute":"local","host":"mac","pid":777},` +
		`"acquired_at":"2026-09-28T11:00:00Z","expires_at":"2026-09-28T11:10:00Z",` +
		`"ttl_s":600,"nonce":"abc","state":"held","module_version":"v2.21.0"}`
	if err := os.WriteFile(filepath.Join(locks, "orders.run.json"), []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
	// A rename-in-progress temp file must not be read as a lease.
	if err := os.WriteFile(filepath.Join(locks, "orders.run.json.tmp.abc"), []byte("{"), 0o644); err != nil {
		t.Fatal(err)
	}

	leases, err := readRunLeases(warehouse)
	if err != nil {
		t.Fatalf("readRunLeases: %v", err)
	}
	if len(leases) != 1 {
		t.Fatalf("got %d leases, want 1", len(leases))
	}
	l := leases[0]
	if l.State != "held" || l.Holder.PID != 777 {
		t.Errorf("lease = %+v, want state held pid 777", l)
	}
	wantExp := time.Date(2026, 9, 28, 11, 10, 0, 0, time.UTC)
	if !l.ExpiresAt.Equal(wantExp) {
		t.Errorf("ExpiresAt = %v, want %v", l.ExpiresAt, wantExp)
	}
	if !l.leaseHeldByOther(wantExp.Add(-time.Minute), 1) {
		t.Error("unexpired held lease for another pid should count as held by other")
	}
	if l.leaseHeldByOther(wantExp.Add(-time.Minute), 777) {
		t.Error("our own pid's lease should not count as held by other")
	}
	if l.leaseHeldByOther(wantExp.Add(time.Minute), 1) {
		t.Error("expired lease should not count as held by other")
	}

	// A malformed lease is ambiguity: the parse fails so the caller keeps
	// the container.
	if err := os.WriteFile(filepath.Join(locks, "broken.run.json"), []byte("not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readRunLeases(warehouse); err == nil {
		t.Error("malformed lease should be an error, got nil")
	}
}

// TestRecordCreatedMetastore checks the registry trims docker's trailing
// newline from the ID, drops empty IDs, and that ReleaseCreatedMetastores
// clears the registry (so a second call has nothing to release).
func TestRecordCreatedMetastore(t *testing.T) {
	createdMetastoresMu.Lock()
	createdMetastores = map[string]createdMetastore{}
	createdMetastoresMu.Unlock()

	recordCreatedMetastore("clavesa-metastore-aaa", releaseTestID+"\n", "/ws/a")
	recordCreatedMetastore("clavesa-metastore-bbb", "  \n", "/ws/b")
	createdMetastoresMu.Lock()
	got := createdMetastores["clavesa-metastore-aaa"]
	n := len(createdMetastores)
	createdMetastoresMu.Unlock()
	if n != 1 {
		t.Fatalf("registry has %d entries, want 1 (empty ID must not be recorded)", n)
	}
	if got.id != releaseTestID || got.root != "/ws/a" {
		t.Errorf("recorded = %+v, want id %q root /ws/a", got, releaseTestID)
	}

	// Clear the registry without docker: the pending entry would make the
	// release probe docker, so drain it the way the release does.
	createdMetastoresMu.Lock()
	createdMetastores = map[string]createdMetastore{}
	createdMetastoresMu.Unlock()
	if removed := ReleaseCreatedMetastores(t.Context()); len(removed) != 0 {
		t.Errorf("empty registry released %v, want nothing", removed)
	}
}
