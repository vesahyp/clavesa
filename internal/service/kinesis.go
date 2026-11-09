package service

import (
	"fmt"
	"sort"

	"github.com/vesahyp/clavesa/internal/graph"
	"github.com/vesahyp/clavesa/internal/orchestration/tfgen"
	"github.com/vesahyp/clavesa/internal/sources"
)

// kinesisDescriptorFields are the descriptor values shared by the local
// (JSON) and cloud (HCL) emit paths. StartingPosition defaults to
// TRIM_HORIZON. Region and Endpoint stay empty when unset: the runner then
// falls back to AWS_REGION and the default endpoint.
func kinesisDescriptorFields(spec sources.Spec) (stream, region, startingPosition, endpoint string) {
	startingPosition = spec.StartingPosition
	if startingPosition == "" {
		startingPosition = sources.KinesisTrimHorizon
	}
	return spec.Stream, spec.Region, startingPosition, spec.Endpoint
}

// kinesisSpecsOf returns the kinesis source specs (deduplicated, by name)
// that the enabled transforms of g read.
func (s *Service) kinesisSpecsOf(g *graph.PipelineGraph) []sources.Spec {
	store := sources.New(s.workspace)
	seen := map[string]bool{}
	var out []sources.Spec
	for _, n := range g.Nodes {
		if n.Type != "transform" || !nodeEnabled(n) {
			continue
		}
		srcInputs, _ := n.Config["source_inputs"].(map[string]interface{})
		for _, raw := range srcInputs {
			name := sourceInputName(raw)
			if name == "" || seen[name] {
				continue
			}
			spec, err := store.Get(name)
			if err != nil || spec.Kind != sources.KindKinesis {
				continue
			}
			seen[name] = true
			out = append(out, spec)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// kinesisStreamsOf is kinesisSpecsOf shaped for the IAM emitter, one entry
// per distinct (stream, region).
func (s *Service) kinesisStreamsOf(g *graph.PipelineGraph) []tfgen.KinesisStream {
	seen := map[tfgen.KinesisStream]bool{}
	var out []tfgen.KinesisStream
	for _, spec := range s.kinesisSpecsOf(g) {
		ks := tfgen.KinesisStream{Name: spec.Stream, Region: spec.Region}
		if !seen[ks] {
			seen[ks] = true
			out = append(out, ks)
		}
	}
	return out
}

// validateKinesisNodes enforces ADR-028 on every enabled transform that has
// a kinesis input: it is the transform's only input, the transform has one
// output, and that output appends.
func (s *Service) validateKinesisNodes(g *graph.PipelineGraph) error {
	store := sources.New(s.workspace)
	for _, n := range g.Nodes {
		if n.Type != "transform" || !nodeEnabled(n) {
			continue
		}
		srcInputs, _ := n.Config["source_inputs"].(map[string]interface{})
		hasKinesis := false
		for _, raw := range srcInputs {
			name := sourceInputName(raw)
			if name == "" {
				continue
			}
			if spec, err := store.Get(name); err == nil && spec.Kind == sources.KindKinesis {
				hasKinesis = true
			}
		}
		if !hasKinesis {
			continue
		}
		extInputs, _ := n.Config["external_inputs"].(map[string]interface{})
		inputs := len(srcInputs) + len(extInputs)
		for _, e := range g.Edges {
			if e.ToNode == n.ID {
				inputs++
			}
		}
		defs, _ := n.Config["output_definitions"].(map[string]interface{})
		if inputs != 1 || len(defs) > 1 {
			return fmt.Errorf("transform %s: a kinesis input must be the transform's only input, and the transform has one output (ADR-028)", n.ID)
		}
		for key := range defs {
			def, _ := defs[key].(map[string]interface{})
			mode, _ := def["mode"].(string)
			if mode == "" && len(outputMergeKeys(n, key)) > 0 {
				mode = "merge"
			}
			if mode != "" && mode != "append" {
				return fmt.Errorf("transform %s: output %q has mode %q; a kinesis transform writes in append mode (ADR-028)", n.ID, key, mode)
			}
		}
	}
	return nil
}
