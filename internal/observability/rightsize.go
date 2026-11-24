package observability

import (
	"fmt"
	"math"
	"sort"
)

// NodeRightsize is a recommend-only memory recommendation for one pipeline
// node, derived from the node's recent run history. Recommend-only: nothing
// here re-deploys or mutates the pipeline — it's the input a human (or a
// future apply path) acts on. Nullable fields use pointers so JSON omits
// them when the history can't support a recommendation (no allocated
// memory_mb on local rows, or no metric-bearing runs yet).
type NodeRightsize struct {
	Node          string  `json:"node"`
	CurrentMB     *int64  `json:"current_mb,omitempty"`
	RecommendedMB *int64  `json:"recommended_mb,omitempty"`
	P95PeakRSSMB  *int64  `json:"p95_peak_rss_mb,omitempty"`
	// Heap-based advice (runner v2.28+ rows). P95HeapAfterGCMB is the p95
	// of node_runs.heap_old_after_gc_mb, HeapMaxMB the newest -Xmx, and
	// RecommendedHeapMB the runner_heap_max_mb the history supports. Set
	// when the node has rows with heap columns; the recommendation then
	// comes from them, not from peak RSS.
	P95HeapAfterGCMB  *int64 `json:"p95_heap_after_gc_mb,omitempty"`
	HeapMaxMB         *int64 `json:"heap_max_mb,omitempty"`
	RecommendedHeapMB *int64 `json:"recommended_heap_mb,omitempty"`
	Samples       int     `json:"samples"`
	SpillRate     float64 `json:"spill_rate"`
	Reason        string  `json:"reason"`
	Confidence    string  `json:"confidence"`
}

// Rightsize groups NodeRun rows by node and computes a per-node memory
// recommendation. Rows are expected newest-first (the providers' node_runs
// SQL orders by started_at DESC); `current` therefore reflects the most
// recent allocation. The aggregation is pure — no I/O, no clock, no
// randomness — so both the service (CLI) and dataquery (HTTP) callers share
// one implementation and the unit tests pin the arithmetic.
//
// Per node:
//   - samples = rows carrying a non-nil PeakRSSMB.
//   - p95 = the ceil(0.95*n)-1 element of the sorted-ascending peak values
//     (clamped into range), i.e. the run a recommendation must not starve.
//   - spillRate = fraction of metric-bearing rows that spilled (memory or
//     disk bytes > 0) — the signal that the node is memory-starved today.
//   - current = the first (newest) non-nil MemoryMB; nil for local rows,
//     which never carry an allocation.
//
// Confidence is "n/a" when there's nothing to recommend against (no current
// allocation, or no metric-bearing samples); otherwise it scales with the
// sample count. Output is sorted by node for stable rendering.
func Rightsize(rows []NodeRun) []NodeRightsize {
	type agg struct {
		peaks     []int64
		heaps     []int64
		spilled   int
		metricRow int
		current   *int64
		heapMax   *int64
	}
	byNode := map[string]*agg{}
	order := []string{}
	for i := range rows {
		r := rows[i]
		a := byNode[r.Node]
		if a == nil {
			a = &agg{}
			byNode[r.Node] = a
			order = append(order, r.Node)
		}
		// current = first (newest) non-nil MemoryMB.
		if a.current == nil && r.MemoryMB != nil {
			v := *r.MemoryMB
			a.current = &v
		}
		if a.heapMax == nil && r.HeapMaxMB != nil {
			v := *r.HeapMaxMB
			a.heapMax = &v
		}
		if r.HeapOldAfterGCMB != nil {
			a.heaps = append(a.heaps, *r.HeapOldAfterGCMB)
		}
		if r.PeakRSSMB != nil {
			a.metricRow++
			a.peaks = append(a.peaks, *r.PeakRSSMB)
			memSpill := r.MemorySpilledBytes != nil && *r.MemorySpilledBytes > 0
			diskSpill := r.DiskSpilledBytes != nil && *r.DiskSpilledBytes > 0
			if memSpill || diskSpill {
				a.spilled++
			}
		}
	}

	out := make([]NodeRightsize, 0, len(order))
	for _, node := range order {
		a := byNode[node]
		nr := NodeRightsize{Node: node, Samples: a.metricRow, CurrentMB: a.current}

		if a.metricRow > 0 {
			nr.SpillRate = float64(a.spilled) / float64(a.metricRow)
			p95 := percentile95(a.peaks)
			nr.P95PeakRSSMB = &p95
		}
		if len(a.heaps) > 0 {
			p95 := percentile95(a.heaps)
			nr.P95HeapAfterGCMB = &p95
			nr.HeapMaxMB = a.heapMax
			heap := recommendHeapMB(p95)
			nr.RecommendedHeapMB = &heap
		}

		switch {
		case a.current == nil:
			nr.Confidence = "n/a"
			nr.Reason = "no allocated memory on record (local runs carry no memory_mb)"
		case len(a.heaps) > 0:
			rec := clampLambdaMB(*nr.RecommendedHeapMB + runnerMemoryOverheadMB)
			nr.RecommendedMB = &rec
			nr.Samples = len(a.heaps)
			nr.Confidence = confidenceFor(len(a.heaps))
			nr.Reason = heapReason(*a.current, rec, *nr.P95HeapAfterGCMB, *nr.RecommendedHeapMB, a.heapMax, len(a.heaps))
		case a.metricRow == 0:
			nr.Confidence = "n/a"
			nr.Reason = "no runs with Spark memory metrics yet"
		default:
			rec := recommendMemoryMB(*nr.P95PeakRSSMB, nr.SpillRate)
			nr.RecommendedMB = &rec
			nr.Confidence = confidenceFor(a.metricRow)
			nr.Reason = rightsizeReason(*a.current, rec, *nr.P95PeakRSSMB, nr.SpillRate, a.metricRow)
		}
		out = append(out, nr)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Node < out[j].Node })
	return out
}

// rightsizeReason renders a one-line human explanation for the
// recommendation. Kept separate from Rightsize so the arithmetic test
// doesn't have to pin prose.
func rightsizeReason(current, recommended, p95 int64, spillRate float64, samples int) string {
	dir := "keep at"
	switch {
	case recommended < current:
		dir = "lower to"
	case recommended > current:
		dir = "raise to"
	}
	spillNote := ""
	if spillRate >= 0.25 {
		spillNote = fmt.Sprintf(", spilling on %.0f%% of runs", spillRate*100)
	}
	return fmt.Sprintf("p95 peak %d MB vs %d MB allocated%s — %s %d MB (n=%d)",
		p95, current, spillNote, dir, recommended, samples)
}

// Heap-based advice. The JVM grows its committed heap toward -Xmx whatever
// the data is, so peak RSS and Lambda's "Max Memory Used" follow the heap
// ceiling, not the need (spikes/gold-memory, 2026-11-22). The need is the
// old generation in use after GC; the heap gives it room twice over: on
// the web-traffic gold bundle 363 MB in use at a 512 MB heap cost 75% more
// GC time, at 768 MB and above it ran as fast as at 1536 MB.
const (
	heapHeadroomFactor = 2
	minHeapMB          = 512
	// runnerMemoryOverheadMB is tfgen's runner_memory_overhead_mb default:
	// the runner Lambda's memory outside the Spark heap.
	runnerMemoryOverheadMB = 1472
)

// recommendHeapMB is the Spark heap (runner_heap_max_mb) for a p95
// old-generation-after-GC value: twice it, rounded up to 64 MB, at least
// 512 MB.
func recommendHeapMB(p95HeapAfterGCMB int64) int64 {
	heap := roundUp64(p95HeapAfterGCMB * heapHeadroomFactor)
	if heap < minHeapMB {
		heap = minHeapMB
	}
	return heap
}

// heapReason renders the one-line explanation of a heap-based
// recommendation.
func heapReason(current, recommended, p95Heap, recHeap int64, heapMax *int64, samples int) string {
	dir := "keep at"
	switch {
	case recommended < current:
		dir = "lower to"
	case recommended > current:
		dir = "raise to"
	}
	maxNote := ""
	if heapMax != nil {
		maxNote = fmt.Sprintf(" of a %d MB heap", *heapMax)
	}
	return fmt.Sprintf("p95 heap in use after GC %d MB%s — heap %d MB + %d MB outside it: %s %d MB (n=%d)",
		p95Heap, maxNote, recHeap, runnerMemoryOverheadMB, dir, recommended, samples)
}

// percentile95 is the ceil(0.95*n)-1 element of the values sorted
// ascending, clamped into range. values must not be empty.
func percentile95(values []int64) int64 {
	sorted := append([]int64(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	n := len(sorted)
	idx := int(math.Ceil(0.95*float64(n))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx > n-1 {
		idx = n - 1
	}
	return sorted[idx]
}

func confidenceFor(samples int) string {
	switch {
	case samples >= 10:
		return "high"
	case samples >= 4:
		return "medium"
	default:
		return "low"
	}
}

func roundUp64(mb int64) int64 {
	const step = 64
	if mb%step != 0 {
		mb = (mb/step + 1) * step
	}
	return mb
}

// clampLambdaMB keeps a Lambda memory size within 512..10240 MB.
func clampLambdaMB(mb int64) int64 {
	if mb < 512 {
		return 512
	}
	if mb > 10240 {
		return 10240
	}
	return mb
}

// recommendMemoryMB turns a p95 peak-RSS observation plus a spill rate into
// a recommended Lambda memory allocation. Pure and unit-tested:
//   - base = p95 * 1.2 (20% headroom over the worst observed run).
//   - if spillRate >= 0.25, bump base by another 25% (the node is
//     memory-starved, give it room rather than chasing the peak).
//   - round UP to the nearest 64 MB (Lambda's allocation granularity).
//   - floor 512 MB (below this Spark cold-starts are flaky), cap 10240 MB
//     (Lambda's max; bigger work belongs on fargate / emr-serverless).
func recommendMemoryMB(p95PeakRSSMB int64, spillRate float64) int64 {
	base := p95PeakRSSMB * 6 / 5
	if spillRate >= 0.25 {
		base = base * 5 / 4
	}
	// Round up to 64 MB step.
	const step = 64
	if base%step != 0 {
		base = (base/step + 1) * step
	}
	if base < 512 {
		base = 512
	}
	if base > 10240 {
		base = 10240
	}
	return base
}
