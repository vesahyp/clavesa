package hosted

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"

	"github.com/vesahyp/clavesa/internal/workspace"
)

// Event sources and detail types the refresh handler acts on.
const (
	sourceSFN       = "aws.states"
	detailSFN       = "Step Functions Execution Status Change"
	sourceS3        = "aws.s3"
	detailS3Created = "Object Created"
	sourceSchedule  = "aws.events"
	detailSchedule  = "Scheduled Event"
)

// RefreshTarget is what one EventBridge event asks for.
type RefreshTarget struct {
	// All is the daily schedule: refresh every workspace.
	All bool
	// Workspace is the one workspace to refresh.
	Workspace string
	// ExecutionARN and Status are set for a Step Functions event. A
	// terminal status means the refresh waits until the run is in the
	// summary (the runs_writer sidecar writes it on the same event).
	ExecutionARN string
	Status       string
	// Time is when the event happened. A stored summary computed after
	// it already shows it, so the refresh is skipped (debounce).
	Time time.Time
}

// Terminal reports whether Status ends an execution.
func (t RefreshTarget) Terminal() bool {
	switch t.Status {
	case "SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED":
		return true
	}
	return false
}

type sfnDetail struct {
	ExecutionARN    string `json:"executionArn"`
	StateMachineARN string `json:"stateMachineArn"`
	Status          string `json:"status"`
}

type s3Detail struct {
	Bucket struct {
		Name string `json:"name"`
	} `json:"bucket"`
	Object struct {
		Key string `json:"key"`
	} `json:"object"`
}

// MapEvent maps one EventBridge event to the workspace it is about. The
// event's account must be a registered cloud workspace's account:
//
//   - Step Functions status change: the state machine name starts with
//     clavesa-<name>- (workspace-scoped AWS names), or the account has
//     exactly one cloud workspace, or the workspace's deployed.json lists
//     the state machine (knownStateMachine, may be nil).
//   - S3 Object Created on _progress/<run>/_run.json: the bucket is the
//     workspace's pipeline bucket.
//   - The daily schedule (aws.events Scheduled Event): every workspace.
//
// An event that maps to no workspace is an error: the bus accepts only
// registered accounts, so it means the registry and the bus disagree.
func MapEvent(reg []Entry, ev events.CloudWatchEvent, knownStateMachine func(e Entry, arn string) bool) (RefreshTarget, error) {
	t := RefreshTarget{Time: ev.Time}
	if ev.Source == sourceSchedule && ev.DetailType == detailSchedule {
		t.All = true
		return t, nil
	}
	var inAccount []Entry
	for _, e := range reg {
		if e.Kind == KindCloud && e.Account == ev.AccountID {
			inAccount = append(inAccount, e)
		}
	}
	if len(inAccount) == 0 {
		return t, fmt.Errorf("event %s from account %s: no registered cloud workspace in that account", ev.ID, ev.AccountID)
	}
	switch {
	case ev.Source == sourceSFN && ev.DetailType == detailSFN:
		var d sfnDetail
		if err := json.Unmarshal(ev.Detail, &d); err != nil {
			return t, fmt.Errorf("event %s: step functions detail: %w", ev.ID, err)
		}
		t.ExecutionARN, t.Status = d.ExecutionARN, d.Status
		smName := d.StateMachineARN[strings.LastIndex(d.StateMachineARN, ":")+1:]
		for _, e := range inAccount {
			if strings.HasPrefix(smName, "clavesa-"+e.Name+"-") {
				t.Workspace = e.Name
				return t, nil
			}
		}
		if knownStateMachine != nil {
			for _, e := range inAccount {
				if knownStateMachine(e, d.StateMachineARN) {
					t.Workspace = e.Name
					return t, nil
				}
			}
		}
		if len(inAccount) == 1 {
			t.Workspace = inAccount[0].Name
			return t, nil
		}
		return t, fmt.Errorf("event %s: state machine %s belongs to no registered workspace of account %s", ev.ID, d.StateMachineARN, ev.AccountID)
	case ev.Source == sourceS3 && ev.DetailType == detailS3Created:
		var d s3Detail
		if err := json.Unmarshal(ev.Detail, &d); err != nil {
			return t, fmt.Errorf("event %s: s3 detail: %w", ev.ID, err)
		}
		for _, e := range inAccount {
			if e.Bucket == d.Bucket.Name {
				t.Workspace = e.Name
				return t, nil
			}
		}
		return t, fmt.Errorf("event %s: bucket %s belongs to no registered workspace of account %s", ev.ID, d.Bucket.Name, ev.AccountID)
	}
	return t, fmt.Errorf("event %s: unsupported event %s / %s", ev.ID, ev.Source, ev.DetailType)
}

// deployedHasStateMachine reports whether the tree copy at root lists the
// state machine in its deployed.json.
func deployedHasStateMachine(root, arn string) bool {
	d, err := workspace.LoadDeployed(root)
	if err != nil || d == nil {
		return false
	}
	for _, p := range d.Pipelines {
		if p.StateMachineARN == arn {
			return true
		}
	}
	return false
}
