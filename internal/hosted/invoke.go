package hosted

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-lambda-go/events"
)

// Invoke is the Lambda handler. One binary has two entry kinds, told
// apart by the event's shape:
//
//   - an HTTP API request (payload format 2.0: "version":"2.0" and
//     requestContext.http) goes to ServeHTTP through the adapter;
//   - an EventBridge event ("detail-type" and "source") goes to Refresh.
//
// Anything else is an error.
func (s *Server) Invoke(ctx context.Context, raw json.RawMessage) (any, error) {
	var probe struct {
		Version        string `json:"version"`
		RequestContext *struct {
			HTTP *struct {
				Method string `json:"method"`
			} `json:"http"`
		} `json:"requestContext"`
		DetailType *string `json:"detail-type"`
		Source     string  `json:"source"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return nil, fmt.Errorf("decode event: %w", err)
	}
	switch {
	case probe.RequestContext != nil && probe.RequestContext.HTTP != nil && probe.Version == "2.0":
		var ev events.APIGatewayV2HTTPRequest
		if err := json.Unmarshal(raw, &ev); err != nil {
			return nil, fmt.Errorf("decode HTTP API event: %w", err)
		}
		return ServeAPIGatewayV2(ctx, s, ev)
	case probe.DetailType != nil && probe.Source != "":
		var ev events.CloudWatchEvent
		if err := json.Unmarshal(raw, &ev); err != nil {
			return nil, fmt.Errorf("decode EventBridge event: %w", err)
		}
		t, err := MapEvent(s.registry, ev, s.knownStateMachine)
		if err != nil {
			s.Logf("%s %v", RefreshFailedMarker, err)
			return nil, err
		}
		return nil, s.Refresh(ctx, t)
	}
	return nil, fmt.Errorf("unsupported event: neither an HTTP API 2.0 request nor an EventBridge event")
}

// knownStateMachine answers from the workspace's deployed.json, opening
// the workspace (and syncing its tree) when needed.
func (s *Server) knownStateMachine(e Entry, arn string) bool {
	ws, err := s.opener.Open(context.Background(), e)
	if err != nil || ws.Root == "" {
		return false
	}
	return deployedHasStateMachine(ws.Root, arn)
}
