package pricing

import (
	"strings"
)

// NodeType is the size of a Databricks node type (an EC2 instance type on
// AWS), used to size the Fargate task a bundle job runs on (ADR-031
// section 7).
type NodeType struct {
	VCPU     float64
	MemoryGB float64
}

// memPerVCPU is GB of memory per vCPU for each instance family Databricks
// offers on AWS. The families are uniform across sizes.
var memPerVCPU = map[string]float64{
	"m4": 4, "m5": 4, "m5a": 4, "m5d": 4, "m5n": 4, "m5dn": 4, "m5zn": 4, "m6i": 4, "m6id": 4, "m6in": 4, "m6idn": 4,
	"m6a": 4, "m6g": 4, "m6gd": 4, "m7g": 4, "m7gd": 4, "m7i": 4, "m7a": 4, "m8g": 4, "m8gd": 4,
	"r4": 7.625, "r5": 8, "r5a": 8, "r5d": 8, "r5n": 8, "r5dn": 8, "r6i": 8, "r6id": 8, "r6in": 8, "r6idn": 8,
	"r6a": 8, "r6g": 8, "r6gd": 8, "r7g": 8, "r7gd": 8, "r7i": 8, "r7a": 8, "r8g": 8, "r8gd": 8,
	"c4": 1.875, "c5": 2, "c5a": 2, "c5d": 2, "c5n": 2.625, "c6i": 2, "c6id": 2, "c6in": 2, "c6a": 2,
	"c6g": 2, "c6gd": 2, "c7g": 2, "c7gd": 2, "c7i": 2, "c7a": 2, "c8g": 2,
	"i3": 7.625, "i3en": 8, "i4i": 8, "i4g": 8, "z1d": 8, "x2idn": 16, "x2iedn": 32,
	"g4dn": 4, "g5": 4, "p3": 7.625, "p4d": 12, "d3": 8, "d3en": 4,
}

// vcpuBySize is the vCPU count of an instance size.
var vcpuBySize = map[string]float64{
	"medium": 1, "large": 2, "xlarge": 4, "2xlarge": 8, "3xlarge": 12, "4xlarge": 16, "6xlarge": 24,
	"8xlarge": 32, "9xlarge": 36, "12xlarge": 48, "16xlarge": 64, "18xlarge": 72, "24xlarge": 96,
	"32xlarge": 128, "48xlarge": 192, "metal": 96,
}

// LookupNodeType returns the size of an AWS node type id such as
// "m5d.large" or "r6id.xlarge". Unknown types return false.
func LookupNodeType(id string) (NodeType, bool) {
	fam, size, ok := strings.Cut(strings.ToLower(id), ".")
	if !ok {
		return NodeType{}, false
	}
	mem, ok1 := memPerVCPU[fam]
	cpu, ok2 := vcpuBySize[size]
	if !ok1 || !ok2 {
		return NodeType{}, false
	}
	return NodeType{VCPU: cpu, MemoryGB: cpu * mem}, true
}

// FargateMaxVCPU and FargateMaxMemoryGB are the largest Fargate task.
const (
	FargateMaxVCPU     = 16
	FargateMaxMemoryGB = 120
)

// FargateSize rounds a size down to a valid Fargate task size: vCPU one of
// 1, 2, 4, 8, 16, and memory inside that vCPU's range at its step.
// It reports false when the size is above the Fargate maximum.
func FargateSize(vcpu, memGB float64) (float64, float64, bool) {
	if vcpu > FargateMaxVCPU || memGB > FargateMaxMemoryGB {
		return 0, 0, false
	}
	type rng struct{ cpu, min, max, step float64 }
	ranges := []rng{{1, 2, 8, 1}, {2, 4, 16, 1}, {4, 8, 30, 1}, {8, 16, 60, 4}, {16, 32, 120, 8}}
	r := ranges[0]
	for _, c := range ranges {
		if c.cpu <= vcpu {
			r = c
		}
	}
	m := memGB
	if m < r.min {
		m = r.min
	}
	if m > r.max {
		m = r.max
	}
	m = r.min + float64(int((m-r.min)/r.step))*r.step
	return r.cpu, m, true
}
