package service

import (
	"context"
	"encoding/json"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	cttypes "github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
)

// scheduleState asks whether a cloud pipeline's schedule rule
// (clavesa-<workspace>-<pipeline>-schedule) is disabled. A rule that was
// paused by hand (aws events disable-rule) means no run is expected, so
// the summary gives a note and no overdue warning.
func (s *Service) scheduleState(ctx context.Context, rule string) (paused bool, since string) {
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
// paused": the summary then says what it said before v2.27.4. The time
// comes from the newest DisableRule event for the rule in CloudTrail
// (90 days); "" when there is none or the look-up is not allowed.
func (s *Service) awsScheduleState(ctx context.Context, rule string) (bool, string) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cfg, err := s.loadAWSConfig(ctx)
	if err != nil {
		return false, ""
	}
	out, err := eventbridge.NewFromConfig(cfg).DescribeRule(ctx, &eventbridge.DescribeRuleInput{Name: aws.String(rule)})
	if err != nil || out.State != ebtypes.RuleStateDisabled {
		return false, ""
	}
	// CloudTrail does not index an EventBridge rule by its name as a
	// resource, so look up DisableRule events and match the rule name in
	// each event's request (newest first).
	ct := cloudtrail.NewFromConfig(cfg)
	in := &cloudtrail.LookupEventsInput{
		LookupAttributes: []cttypes.LookupAttribute{{
			AttributeKey:   cttypes.LookupAttributeKeyEventName,
			AttributeValue: aws.String("DisableRule"),
		}},
		MaxResults: aws.Int32(50),
	}
	for page := 0; page < 5; page++ {
		out, err := ct.LookupEvents(ctx, in)
		if err != nil {
			return true, ""
		}
		if since := disabledSince(out.Events, rule); since != "" {
			return true, since
		}
		if out.NextToken == nil {
			break
		}
		in.NextToken = out.NextToken
	}
	return true, ""
}

// disabledSince returns the time (RFC 3339, UTC) of the newest DisableRule
// event in events whose request names rule, or "".
func disabledSince(events []cttypes.Event, rule string) string {
	for _, e := range events { // newest first
		if aws.ToString(e.EventName) != "DisableRule" || e.EventTime == nil {
			continue
		}
		var body struct {
			RequestParameters struct {
				Name string `json:"name"`
			} `json:"requestParameters"`
		}
		if json.Unmarshal([]byte(aws.ToString(e.CloudTrailEvent)), &body) != nil {
			continue
		}
		if body.RequestParameters.Name == rule {
			return e.EventTime.UTC().Format(time.RFC3339)
		}
	}
	return ""
}
