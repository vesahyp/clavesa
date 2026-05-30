package observability

import "testing"

func i64(v int64) *int64 { return &v }

func TestRightsize(t *testing.T) {
	tests := []struct {
		name           string
		rows           []NodeRun
		wantConfidence string
		wantSamples    int
		// recCheck is run against the recommended value when non-nil.
		recCheck func(t *testing.T, rec *int64)
		// p95Check is run against the p95 value when non-nil.
		p95Check func(t *testing.T, p95 *int64)
	}{
		{
			name: "over-provisioned recommends down, high confidence",
			rows: overProvisionedRows(12),
			// 12 metric rows → high.
			wantConfidence: "high",
			wantSamples:    12,
			recCheck: func(t *testing.T, rec *int64) {
				if rec == nil {
					t.Fatal("expected a recommendation")
				}
				// peak 800, no spill → 800*1.2=960, round-up 64 → 960; floor 512.
				if *rec != 960 {
					t.Fatalf("recommended = %d, want 960", *rec)
				}
				if *rec >= 2048 {
					t.Fatalf("expected recommendation below current 2048, got %d", *rec)
				}
			},
		},
		{
			name:           "spill-heavy recommends up",
			rows:           spillHeavyRows(),
			wantConfidence: "medium", // 5 metric rows → medium.
			wantSamples:    5,
			recCheck: func(t *testing.T, rec *int64) {
				if rec == nil {
					t.Fatal("expected a recommendation")
				}
				// All 5 rows spill, peak 1900, current 2048.
				// base = 1900*1.2 = 2280; spill bump *1.25 = 2850; round-up 64 → 2880.
				if *rec != 2880 {
					t.Fatalf("recommended = %d, want 2880", *rec)
				}
				if *rec <= 2048 {
					t.Fatalf("expected recommendation above current 2048, got %d", *rec)
				}
			},
		},
		{
			name: "local nil MemoryMB → n/a",
			rows: []NodeRun{
				{Node: "trips", PeakRSSMB: i64(700)},
				{Node: "trips", PeakRSSMB: i64(710)},
			},
			wantConfidence: "n/a",
			wantSamples:    2,
		},
		{
			name: "no metric rows → samples 0, n/a",
			rows: []NodeRun{
				{Node: "trips", MemoryMB: i64(1024)},
				{Node: "trips", MemoryMB: i64(1024)},
			},
			wantConfidence: "n/a",
			wantSamples:    0,
			p95Check: func(t *testing.T, p95 *int64) {
				if p95 != nil {
					t.Fatalf("expected nil p95 with no metric rows, got %d", *p95)
				}
			},
		},
		{
			name: "few samples → low confidence",
			rows: []NodeRun{
				{Node: "trips", MemoryMB: i64(1024), PeakRSSMB: i64(600)},
				{Node: "trips", MemoryMB: i64(1024), PeakRSSMB: i64(620)},
			},
			wantConfidence: "low",
			wantSamples:    2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Rightsize(tt.rows)
			if len(got) != 1 {
				t.Fatalf("expected 1 node, got %d", len(got))
			}
			nr := got[0]
			if nr.Confidence != tt.wantConfidence {
				t.Errorf("confidence = %q, want %q", nr.Confidence, tt.wantConfidence)
			}
			if nr.Samples != tt.wantSamples {
				t.Errorf("samples = %d, want %d", nr.Samples, tt.wantSamples)
			}
			if tt.recCheck != nil {
				tt.recCheck(t, nr.RecommendedMB)
			}
			if tt.p95Check != nil {
				tt.p95Check(t, nr.P95PeakRSSMB)
			}
			if nr.Reason == "" {
				t.Error("expected a non-empty reason")
			}
		})
	}
}

// TestRightsizeCurrentIsNewest pins that `current` is taken from the newest
// row (rows arrive started_at DESC).
func TestRightsizeCurrentIsNewest(t *testing.T) {
	rows := []NodeRun{
		{Node: "n", MemoryMB: i64(2048), PeakRSSMB: i64(500)}, // newest
		{Node: "n", MemoryMB: i64(1024), PeakRSSMB: i64(500)}, // older
	}
	got := Rightsize(rows)
	if got[0].CurrentMB == nil || *got[0].CurrentMB != 2048 {
		t.Fatalf("current = %v, want 2048 (newest row wins)", got[0].CurrentMB)
	}
}

// TestRightsizeMultiNodeSorted pins node grouping + sorted output.
func TestRightsizeMultiNodeSorted(t *testing.T) {
	rows := []NodeRun{
		{Node: "zeta", MemoryMB: i64(1024), PeakRSSMB: i64(500)},
		{Node: "alpha", MemoryMB: i64(1024), PeakRSSMB: i64(500)},
	}
	got := Rightsize(rows)
	if len(got) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(got))
	}
	if got[0].Node != "alpha" || got[1].Node != "zeta" {
		t.Fatalf("nodes not sorted: %q, %q", got[0].Node, got[1].Node)
	}
}

func TestRecommendMemoryMB(t *testing.T) {
	tests := []struct {
		name      string
		p95       int64
		spillRate float64
		want      int64
	}{
		{"basic headroom, round up", 800, 0, 960},          // 800*1.2=960
		{"floor at 512", 100, 0, 512},                      // 100*1.2=120 → floor 512
		{"cap at 10240", 20000, 0, 10240},                  // 20000*1.2=24000 → cap
		{"spill bump applied at 0.25", 1000, 0.25, 1536},   // 1000*1.2=1200*1.25=1500 → round-up 1536
		{"spill bump not applied below 0.25", 1000, 0.2, 1216}, // 1200 → round-up 1216
		{"round up to 64 step", 513, 0, 640},               // 513*1.2=615.6→615 → round-up 640
		{"cap survives spill bump", 9000, 0.5, 10240},      // 9000*1.2=10800*1.25 → cap
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := recommendMemoryMB(tt.p95, tt.spillRate)
			if got != tt.want {
				t.Errorf("recommendMemoryMB(%d, %v) = %d, want %d", tt.p95, tt.spillRate, got, tt.want)
			}
			if got%64 != 0 {
				t.Errorf("recommendation %d is not a multiple of 64", got)
			}
		})
	}
}

// overProvisionedRows returns n metric-bearing rows with a low peak (800),
// no spill, allocated at 2048 MB — the textbook "lower this" case.
func overProvisionedRows(n int) []NodeRun {
	rows := make([]NodeRun, n)
	for i := range rows {
		rows[i] = NodeRun{
			Node:      "trips",
			MemoryMB:  i64(2048),
			PeakRSSMB: i64(800),
		}
	}
	return rows
}

// spillHeavyRows returns 5 metric rows all spilling, peak 1900, allocated
// 2048 MB — the "raise this" case.
func spillHeavyRows() []NodeRun {
	rows := make([]NodeRun, 5)
	for i := range rows {
		rows[i] = NodeRun{
			Node:               "agg",
			MemoryMB:           i64(2048),
			PeakRSSMB:          i64(1900),
			MemorySpilledBytes: i64(1 << 20),
			DiskSpilledBytes:   i64(1 << 20),
		}
	}
	return rows
}
