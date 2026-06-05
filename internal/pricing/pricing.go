// Package pricing turns runner invocation telemetry (node_runs rows) into an
// approximate billed-compute dollar figure and a "cost per billion records"
// efficiency metric.
//
// Scope and accuracy (v1): the price table below is AWS us-east-1 on-demand,
// x86, hard-coded. It is an approximation — region-fixed, ignores Savings
// Plans / committed-use discounts, Lambda request charges, and per-run vCPU
// (node_runs does not carry vCPU today; see CostUSD). Treat the output as a
// relative comparison aid across nodes/runs, not a billing reconciliation.
//
// Dependency note: CostUSD takes the primitive inputs it needs (target,
// durationMs, memoryMB) rather than an observability.NodeRun, so the
// per-run cost math has no package dependency at all. Only AggregateCost
// reaches into observability (to read the NodeRun fields) — that import is
// one-directional (observability does not import pricing), so there is no
// import cycle either way; primitives are simply the leaner choice.
package pricing

// AWS us-east-1 on-demand pricing, x86. Source:
//   - Lambda:         https://aws.amazon.com/lambda/pricing/
//   - Fargate:        https://aws.amazon.com/fargate/pricing/
//   - EMR Serverless: https://aws.amazon.com/emr/pricing/ (EMR Serverless tab)
// Approximation only, region-fixed for v1 (see package doc).
const (
	// lambdaGBSecond is the per-GB-second compute price. Lambda also bills
	// $0.20 per 1M requests; excluded here as negligible relative to
	// GB-second compute for the workloads clavesa runs (a note, not a bug).
	lambdaGBSecond = 0.0000166667

	// Fargate bills vCPU and memory separately, per hour.
	fargateVCPUHour = 0.04048
	fargateGBHour   = 0.004445

	// EMR Serverless bills vCPU and memory separately, per hour.
	emrServerlessVCPUHour = 0.052624
	emrServerlessGBHour   = 0.0057785
)

// Memory-per-vCPU floor used to derive a vCPU count from allocated memory for
// the targets that bill vCPU separately. node_runs does not capture per-run
// vCPU today, so this is a best-effort proxy: Fargate and EMR Serverless both
// allow configurations down to ~2 GB per vCPU, which is the cheapest (and so
// most conservative) assumption. Revisit once the runner records actual vCPU.
const (
	fargateMemPerVCPUGB       = 2.0
	emrServerlessMemPerVCPUGB = 2.0
)

// PriceBasis is the human-readable provenance string surfaced on PipelineCost.
const PriceBasis = "AWS us-east-1 on-demand (approx); local=$0"

// Compute target identifiers (mirrors internal/observability ComputeTarget
// values; kept as local constants so pricing has no dependency on it).
const (
	TargetLocal         = "local"
	TargetLambda        = "lambda"
	TargetFargate       = "fargate"
	TargetEMRServerless = "emr-serverless"
)

// CostUSD returns the approximate billed compute cost in USD for one runner
// invocation, given its compute target, wall-clock duration, and allocated
// memory.
//
//   - local                  → 0 (no metered compute).
//   - lambda                 → (durationMs/1000) * (memoryMB/1024) * lambdaGBSecond.
//   - fargate / emr-serverless → vCPU-seconds + GB-seconds, with vCPU derived
//     from memory via the target's mem-per-vCPU floor (approximate; see above).
//
// memoryMB is nilable (local rows and pre-metric runner rows carry no
// allocation). When it is nil or non-positive the cost is 0 for every target
// except where memory is the only input — for all metered targets here memory
// is required, so a missing allocation yields 0 (and that's fine: an
// un-costable run contributes nothing rather than a wrong number).
func CostUSD(target string, durationMs int64, memoryMB *int64) float64 {
	if target == TargetLocal || durationMs <= 0 {
		return 0
	}
	if memoryMB == nil || *memoryMB <= 0 {
		return 0
	}
	seconds := float64(durationMs) / 1000.0
	memGB := float64(*memoryMB) / 1024.0

	switch target {
	case TargetLambda:
		return seconds * memGB * lambdaGBSecond
	case TargetFargate:
		vcpu := memGB / fargateMemPerVCPUGB
		hours := seconds / 3600.0
		return vcpu*hours*fargateVCPUHour + memGB*hours*fargateGBHour
	case TargetEMRServerless:
		vcpu := memGB / emrServerlessMemPerVCPUGB
		hours := seconds / 3600.0
		return vcpu*hours*emrServerlessVCPUHour + memGB*hours*emrServerlessGBHour
	default:
		// Unknown target — no price table entry, contribute nothing.
		return 0
	}
}
