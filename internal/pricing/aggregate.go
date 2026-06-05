package pricing

import "github.com/vesahyp/clavesa/internal/observability"

// NodeCost is the cost + throughput rollup for one pipeline node, aggregated
// over that node's completed runs.
type NodeCost struct {
	Node           string  `json:"node"`
	ComputeTarget  string  `json:"computeTarget"`
	Runs           int     `json:"runs"`
	Records        int64   `json:"records"`
	BilledSeconds  float64 `json:"billedSeconds"`
	CostUSD        float64 `json:"costUsd"`
	RecordsPerSec  float64 `json:"recordsPerSec"`
	CostPerBillion float64 `json:"costPerBillion"`
}

// PipelineCost is the cost-per-billion-records rollup for a whole pipeline.
type PipelineCost struct {
	Pipeline       string     `json:"pipeline"`
	TotalRecords   int64      `json:"totalRecords"`
	TotalCostUSD   float64    `json:"totalCostUsd"`
	CostPerBillion float64    `json:"costPerBillion"`
	RecordsPerSec  float64    `json:"recordsPerSec"`
	PerNode        []NodeCost `json:"perNode"`
	PriceBasis     string     `json:"priceBasis"`
}

// AggregateCost rolls a slice of node_runs rows into a PipelineCost. Pure: no
// I/O, no clock, no randomness, so the CLI and HTTP callers share one
// implementation and the unit tests pin the arithmetic.
//
// Only completed runs are aggregated. node_runs' completed convention is
// Status == "ok" (the same literal the dashboard grid checks `=== "ok"`);
// every other status (running, error, skipped) is excluded so partial or
// failed compute does not skew cost-per-record.
//
// Records processed per run = InputRecords when non-nil and > 0, else
// OutputRows as a fallback (an aggregation/output-only transform may not
// report input_records). A run with neither contributes 0 records but still
// contributes its cost — so an un-instrumented but billed run correctly drags
// the efficiency metric rather than vanishing.
//
// Derived metrics:
//   - CostPerBillion = TotalCostUSD / TotalRecords * 1e9 (0 when TotalRecords == 0).
//   - RecordsPerSec  = TotalRecords / sum(DurationMs/1000) (0 when no duration).
//
// PerNode is sorted by first-seen order of the (newest-first) input so output
// is deterministic for a given input ordering. ComputeTarget on a NodeCost is
// the last non-empty target seen for that node.
func AggregateCost(pipeline string, rows []observability.NodeRun) PipelineCost {
	type agg struct {
		target  string
		runs    int
		records int64
		seconds float64
		cost    float64
	}
	byNode := map[string]*agg{}
	order := []string{}

	var totalRecords int64
	var totalCost float64
	var totalSeconds float64

	for i := range rows {
		r := rows[i]
		if r.Status != "ok" {
			continue
		}
		a := byNode[r.Node]
		if a == nil {
			a = &agg{}
			byNode[r.Node] = a
			order = append(order, r.Node)
		}
		if r.ComputeTarget != "" {
			a.target = r.ComputeTarget
		}
		a.runs++

		records := recordsFor(r)
		a.records += records
		totalRecords += records

		var durMs int64
		if r.DurationMs != nil {
			durMs = *r.DurationMs
		}
		secs := float64(durMs) / 1000.0
		a.seconds += secs
		totalSeconds += secs

		c := CostUSD(r.ComputeTarget, durMs, r.MemoryMB)
		a.cost += c
		totalCost += c
	}

	perNode := make([]NodeCost, 0, len(order))
	for _, node := range order {
		a := byNode[node]
		nc := NodeCost{
			Node:          node,
			ComputeTarget: a.target,
			Runs:          a.runs,
			Records:       a.records,
			BilledSeconds: a.seconds,
			CostUSD:       a.cost,
		}
		if a.seconds > 0 {
			nc.RecordsPerSec = float64(a.records) / a.seconds
		}
		if a.records > 0 {
			nc.CostPerBillion = a.cost / float64(a.records) * 1e9
		}
		perNode = append(perNode, nc)
	}

	pc := PipelineCost{
		Pipeline:     pipeline,
		TotalRecords: totalRecords,
		TotalCostUSD: totalCost,
		PerNode:      perNode,
		PriceBasis:   PriceBasis,
	}
	if totalSeconds > 0 {
		pc.RecordsPerSec = float64(totalRecords) / totalSeconds
	}
	if totalRecords > 0 {
		pc.CostPerBillion = totalCost / float64(totalRecords) * 1e9
	}
	return pc
}

// recordsFor returns the records-processed count for one run: InputRecords
// when present and positive, else OutputRows as a fallback, else 0.
func recordsFor(r observability.NodeRun) int64 {
	if r.InputRecords != nil && *r.InputRecords > 0 {
		return *r.InputRecords
	}
	if r.OutputRows != nil && *r.OutputRows > 0 {
		return *r.OutputRows
	}
	return 0
}
