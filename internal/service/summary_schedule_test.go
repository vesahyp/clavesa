package service

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cttypes "github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"
)

// TestDisabledSince: the pause time is the newest DisableRule event whose
// request names the rule; other rules and other events do not count.
func TestDisabledSince(t *testing.T) {
	at := func(s string) *time.Time { v, _ := time.Parse(time.RFC3339, s); return &v }
	ev := func(name, when, body string) cttypes.Event {
		return cttypes.Event{EventName: aws.String(name), EventTime: at(when), CloudTrailEvent: aws.String(body)}
	}
	events := []cttypes.Event{
		ev("DisableRule", "2026-11-12T10:00:00Z", `{"requestParameters":{"name":"clavesa-other-schedule"}}`),
		ev("EnableRule", "2026-11-11T10:00:00Z", `{"requestParameters":{"name":"clavesa-ucguide-guide_events-schedule"}}`),
		ev("DisableRule", "2026-11-09T07:21:11Z", `{"requestParameters":{"name":"clavesa-ucguide-guide_events-schedule"}}`),
		ev("DisableRule", "2026-11-01T07:00:00Z", `{"requestParameters":{"name":"clavesa-ucguide-guide_events-schedule"}}`),
	}
	if got := disabledSince(events, "clavesa-ucguide-guide_events-schedule"); got != "2026-11-09T07:21:11Z" {
		t.Errorf("since = %q, want the newest matching DisableRule", got)
	}
	if got := disabledSince(events, "clavesa-none-schedule"); got != "" {
		t.Errorf("no match = %q, want \"\"", got)
	}
	if got := pausedNote("2026-11-09T07:21:11Z"); got != "schedule paused since 9.11 09:21" {
		t.Errorf("note = %q", got)
	}
}
