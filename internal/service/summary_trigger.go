package service

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/vesahyp/clavesa/internal/hclparser"
)

// Trigger kinds of a pipeline in the summary: what starts its runs, and so
// how often a run is expected.
const (
	TriggerSchedule = "schedule" // trigger_schedule: an EventBridge schedule
	TriggerPoller   = "poller"   // trigger_batch_window: the source-queue poller
	TriggerUpstream = "upstream" // started when an upstream pipeline succeeds
)

// minPollerInterval is the shortest poller window that implies a run per
// window. A poller that looks every minute starts a run only when data
// has arrived, so a quiet minute says nothing; a poller that drains a
// day of CloudFront logs at 06:10 (web-traffic) runs every day.
const minPollerInterval = time.Hour

// pollerQueuesRe finds the poller's queue list in an emitted
// orchestration.tf: `_poller_queue_arns = [module.src.trigger_queue_arn]`.
var pollerQueuesRe = regexp.MustCompile(`(?m)^\s*_poller_queue_arns\s*=\s*\[([^\]]*)\]`)

// pipelineTrigger is what starts one pipeline and the interval it implies.
type pipelineTrigger struct {
	kind     string
	expr     string // the schedule or poller expression; "" for upstream
	interval time.Duration
	after    []string // upstream: the producer pipelines it inherits from
	// paused: no run is expected (cloud only). A schedule or poller whose
	// EventBridge rule is disabled, or an upstream pipeline whose every
	// upstream is paused.
	paused bool
}

// hasPoller reports whether the pipeline's emitted orchestration.tf has a
// poller with at least one queue. It reads the emitted file, the one that
// is deployed (and the one the hosted tree copy carries), not the source
// registry, which only the Mac has.
func hasPoller(pipelineDir string) bool {
	b, err := os.ReadFile(filepath.Join(pipelineDir, "orchestration.tf"))
	if err != nil {
		return false
	}
	m := pollerQueuesRe.FindSubmatch(b)
	return m != nil && strings.TrimSpace(string(m[1])) != ""
}

// pipelineTriggers works out every pipeline's trigger, keyed by name:
//
//  1. trigger_schedule, when declared;
//  2. else trigger_batch_window, when the pipeline has a poller and the
//     window is at least minPollerInterval;
//  3. else the shortest interval of the upstream pipelines it reads from
//     (cloud only: there an upstream success starts it through an
//     EventBridge rule), so silver and gold of web-traffic are expected
//     daily because bronze is.
//
// A pipeline with none of these has no expected interval.
func (s *Service) pipelineTriggers(pipes []PipelineInfo, cloud bool) map[string]pipelineTrigger {
	out := map[string]pipelineTrigger{}
	for _, p := range pipes {
		vars, err := s.GetVars(p.Dir)
		if err != nil {
			continue
		}
		if sched := tfString(vars["trigger_schedule"]); sched != "" {
			out[p.Name] = pipelineTrigger{kind: TriggerSchedule, expr: sched, interval: scheduleInterval(sched)}
			continue
		}
		if win := tfString(vars["trigger_batch_window"]); win != "" && hasPoller(s.resolveDir(p.Dir)) {
			if iv := scheduleInterval(win); iv >= minPollerInterval {
				out[p.Name] = pipelineTrigger{kind: TriggerPoller, expr: win, interval: iv}
			}
		}
	}
	if !cloud {
		return out
	}
	// Upstream inheritance, repeated so a chain (bronze → silver → gold)
	// resolves whatever the order of the pipelines.
	upstream := map[string][]string{}
	for _, p := range pipes {
		if _, ok := out[p.Name]; ok {
			continue
		}
		abs := s.resolveDir(p.Dir)
		g, err := hclparser.Parse(abs)
		if err != nil {
			continue
		}
		if ups := s.upstreamProducerPipelines(g, filepath.Base(abs), resolvePipelineSchema(abs, filepath.Base(abs))); len(ups) > 0 {
			upstream[p.Name] = ups
		}
	}
	for changed := true; changed; {
		changed = false
		for name, ups := range upstream {
			if _, ok := out[name]; ok {
				continue
			}
			var best time.Duration
			var after []string
			for _, u := range ups {
				t, ok := out[u]
				if !ok || t.interval <= 0 {
					continue
				}
				after = append(after, u)
				if best == 0 || t.interval < best {
					best = t.interval
				}
			}
			if best > 0 {
				sort.Strings(after)
				out[name] = pipelineTrigger{kind: TriggerUpstream, interval: best, after: after}
				changed = true
			}
		}
	}
	return out
}

// triggerPauses fills in paused for every trigger: the rule state of each
// schedule and poller (clavesa-<ws>-<pipeline>-schedule or -poller), then
// upstream pipelines, repeated so a chain resolves in any order.
func (s *Service) triggerPauses(ctx context.Context, triggers map[string]pipelineTrigger) {
	for name, t := range triggers {
		switch t.kind {
		case TriggerSchedule:
			t.paused = s.scheduleState(ctx, s.awsName(name)+"-schedule")
		case TriggerPoller:
			t.paused = s.scheduleState(ctx, s.awsName(name)+"-poller")
		default:
			continue
		}
		triggers[name] = t
	}
	for changed := true; changed; {
		changed = false
		for name, t := range triggers {
			if t.kind != TriggerUpstream || t.paused || len(t.after) == 0 {
				continue
			}
			all := true
			for _, u := range t.after {
				all = all && triggers[u].paused
			}
			if all {
				t.paused = true
				triggers[name] = t
				changed = true
			}
		}
	}
}

// tfString is a tfvars value as a plain string: "" for null or empty.
func tfString(v string) string {
	v = strings.TrimSpace(v)
	if v == "null" {
		return ""
	}
	return v
}
