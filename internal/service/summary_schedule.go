package service

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
)

// scheduleState asks whether a cloud pipeline's schedule or poller rule
// (clavesa-<workspace>-<pipeline>-schedule or -poller) is disabled. A rule
// that was paused by hand (aws events disable-rule) means no run is
// expected, so the summary gives a note and no overdue warning. When it
// was paused is not looked up: that needs CloudTrail, an account-wide read
// the reader role does not get (Vesa, 2026-11-17).
func (s *Service) scheduleState(ctx context.Context, rule string) bool {
	if s.scheduleStateFn != nil {
		return s.scheduleStateFn(ctx, rule)
	}
	return scheduleLookup(s, ctx, rule)
}

// scheduleLookup is the default look-up. The package's TestMain swaps it
// for one that never reaches AWS.
var scheduleLookup = (*Service).awsScheduleState

// awsScheduleState is the real look-up. Any error (no credentials, a
// reader role without events:DescribeRule, no such rule) counts as "not
// paused": the summary then says what it said before v2.27.4.
func (s *Service) awsScheduleState(ctx context.Context, rule string) bool {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cfg, err := s.loadAWSConfig(ctx)
	if err != nil {
		return false
	}
	out, err := eventbridge.NewFromConfig(cfg).DescribeRule(ctx, &eventbridge.DescribeRuleInput{Name: aws.String(rule)})
	return err == nil && out.State == ebtypes.RuleStateDisabled
}
