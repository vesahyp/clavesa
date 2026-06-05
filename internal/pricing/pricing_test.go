package pricing

import (
	"math"
	"testing"

	"github.com/vesahyp/clavesa/internal/observability"
)

func i64(v int64) *int64 { return &v }

func TestCostUSD(t *testing.T) {
	tests := []struct {
		name      string
		target    string
		durMs     int64
		memMB     *int64
		want      float64 // exact when >0; for fargate/emr we only assert >0 via wantPos
		wantPos   bool    // when true, assert >0 instead of exact
		tolerance float64
	}{
		{
			name:   "local is free",
			target: TargetLocal,
			durMs:  60000,
			memMB:  i64(2048),
			want:   0,
		},
		{
			name:   "lambda nil memory is zero",
			target: TargetLambda,
			durMs:  60000,
			memMB:  nil,
			want:   0,
		},
		{
			name:   "lambda zero duration is zero",
			target: TargetLambda,
			durMs:  0,
			memMB:  i64(1024),
			want:   0,
		},
		{
			// 10s @ 1024MB(=1GB): 10 * 1 * 0.0000166667 = 0.000166667
			name:      "lambda known case",
			target:    TargetLambda,
			durMs:     10000,
			memMB:     i64(1024),
			want:      10.0 * 1.0 * lambdaGBSecond,
			tolerance: 1e-12,
		},
		{
			name:    "fargate is positive",
			target:  TargetFargate,
			durMs:   3600000, // 1h
			memMB:   i64(4096),
			wantPos: true,
		},
		{
			name:    "emr-serverless is positive",
			target:  TargetEMRServerless,
			durMs:   3600000,
			memMB:   i64(4096),
			wantPos: true,
		},
		{
			name:   "unknown target is zero",
			target: "glue",
			durMs:  60000,
			memMB:  i64(2048),
			want:   0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := CostUSD(tc.target, tc.durMs, tc.memMB)
			if tc.wantPos {
				if got <= 0 {
					t.Fatalf("expected positive cost, got %v", got)
				}
				return
			}
			tol := tc.tolerance
			if tol == 0 {
				tol = 1e-15
			}
			if math.Abs(got-tc.want) > tol {
				t.Fatalf("cost = %v, want %v (tol %v)", got, tc.want, tol)
			}
		})
	}
}

// fargate cost should equal vCPU-seconds + GB-seconds with the 2GB/vCPU floor.
func TestCostUSDFargateArithmetic(t *testing.T) {
	// 1h @ 4096MB(=4GB) → vcpu = 4/2 = 2; 1h.
	got := CostUSD(TargetFargate, 3600000, i64(4096))
	want := 2.0*1.0*fargateVCPUHour + 4.0*1.0*fargateGBHour
	if math.Abs(got-want) > 1e-12 {
		t.Fatalf("fargate cost = %v, want %v", got, want)
	}
}

func TestAggregateCost(t *testing.T) {
	// Two nodes on lambda. Node A reports input_records; node B reports only
	// output_rows (fallback path). One non-ok run on A must be excluded.
	rows := []observability.NodeRun{
		{
			Node:          "a",
			Status:        "ok",
			ComputeTarget: TargetLambda,
			DurationMs:    i64(10000), // 10s
			MemoryMB:      i64(1024),  // 1GB
			InputRecords:  i64(1_000_000),
		},
		{
			Node:          "a",
			Status:        "error", // excluded
			ComputeTarget: TargetLambda,
			DurationMs:    i64(10000),
			MemoryMB:      i64(1024),
			InputRecords:  i64(9_999_999),
		},
		{
			Node:          "b",
			Status:        "ok",
			ComputeTarget: TargetLambda,
			DurationMs:    i64(10000), // 10s
			MemoryMB:      i64(1024),  // 1GB
			// no InputRecords → falls back to OutputRows
			OutputRows: i64(1_000_000),
		},
	}

	pc := AggregateCost("demo", rows)

	if pc.Pipeline != "demo" {
		t.Fatalf("pipeline = %q", pc.Pipeline)
	}
	if pc.PriceBasis != PriceBasis {
		t.Fatalf("price basis = %q", pc.PriceBasis)
	}
	if len(pc.PerNode) != 2 {
		t.Fatalf("expected 2 nodes (error run excluded), got %d", len(pc.PerNode))
	}

	// Total records = 1M (a, input) + 1M (b, output fallback) = 2M.
	if pc.TotalRecords != 2_000_000 {
		t.Fatalf("total records = %d, want 2000000", pc.TotalRecords)
	}

	// Each ok run: 10s * 1GB * lambdaGBSecond. Two ok runs.
	perRun := 10.0 * 1.0 * lambdaGBSecond
	wantCost := 2 * perRun
	if math.Abs(pc.TotalCostUSD-wantCost) > 1e-12 {
		t.Fatalf("total cost = %v, want %v", pc.TotalCostUSD, wantCost)
	}

	// CostPerBillion = totalCost / 2e6 * 1e9.
	wantCPB := wantCost / 2_000_000.0 * 1e9
	if math.Abs(pc.CostPerBillion-wantCPB) > 1e-9 {
		t.Fatalf("cost per billion = %v, want %v", pc.CostPerBillion, wantCPB)
	}

	// Node b proves the output_rows fallback worked.
	var nodeB *NodeCost
	for i := range pc.PerNode {
		if pc.PerNode[i].Node == "b" {
			nodeB = &pc.PerNode[i]
		}
	}
	if nodeB == nil {
		t.Fatal("node b missing")
	}
	if nodeB.Records != 1_000_000 {
		t.Fatalf("node b records (output_rows fallback) = %d, want 1000000", nodeB.Records)
	}

	// RecordsPerSec = 2M / 20s = 100k.
	if math.Abs(pc.RecordsPerSec-100_000.0) > 1e-6 {
		t.Fatalf("records/sec = %v, want 100000", pc.RecordsPerSec)
	}
}

func TestAggregateCostEmpty(t *testing.T) {
	pc := AggregateCost("demo", nil)
	if pc.TotalRecords != 0 || pc.CostPerBillion != 0 || pc.RecordsPerSec != 0 {
		t.Fatalf("empty aggregate should be zeroed, got %+v", pc)
	}
	if pc.PriceBasis != PriceBasis {
		t.Fatalf("price basis = %q", pc.PriceBasis)
	}
}
