package hosted

import (
	"context"
	"strings"
)

// A Viewer is the signed-in user's groups (ADR-035): "all" sees every
// workspace, "ws-<name>" sees that one. The API's Cognito JWT authorizer
// has already verified the token whose claims these are.
type Viewer struct {
	Groups map[string]bool
}

// GroupAll is the group that sees every workspace.
const GroupAll = "all"

// WorkspaceGroup is the group of one workspace's viewers.
func WorkspaceGroup(name string) string { return "ws-" + name }

// Sees reports whether v may see workspace name. A nil viewer sees
// nothing.
func (v *Viewer) Sees(name string) bool {
	return v != nil && (v.Groups[GroupAll] || v.Groups[WorkspaceGroup(name)])
}

// All reports whether v is in the group that sees every workspace.
func (v *Viewer) All() bool { return v != nil && v.Groups[GroupAll] }

// viewerFromClaims reads cognito:groups from JWT claims as the HTTP API
// passes them: "[all ws-smoke2]" (space separated in brackets), with a
// comma-separated form accepted too. No claim gives a viewer with no
// groups, which sees nothing.
func viewerFromClaims(claims map[string]string) *Viewer {
	v := &Viewer{Groups: map[string]bool{}}
	raw := strings.Trim(strings.TrimSpace(claims["cognito:groups"]), "[]")
	for _, g := range strings.FieldsFunc(raw, func(r rune) bool { return r == ' ' || r == ',' }) {
		if g = strings.Trim(g, `"`); g != "" {
			v.Groups[g] = true
		}
	}
	return v
}

type viewerKey struct{}

// withViewer and viewerFrom carry the viewer on a request's context.
func withViewer(ctx context.Context, v *Viewer) context.Context {
	return context.WithValue(ctx, viewerKey{}, v)
}

func viewerFrom(ctx context.Context) *Viewer {
	v, _ := ctx.Value(viewerKey{}).(*Viewer)
	return v
}
