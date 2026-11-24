package hosted

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/service"
)

// TestAdminView (column tags): a viewer in the group "all" opens a cloud
// workspace with its admin role; a workspace viewer opens it with the
// reader role; a workspace with no admin role is read with the reader
// role by everyone.
func TestAdminView(t *testing.T) {
	reg, err := ParseRegistry([]byte(`[
 {"name":"smoke2","kind":"cloud","account":"699166197771","region":"eu-north-1",
  "reader_role_arn":"arn:aws:iam::699166197771:role/clavesa-ui-reader-smoke2","bucket":"smoke2-clavesa",
  "admin_role_arn":"arn:aws:iam::699166197771:role/clavesa-ui-admin-smoke2"},
 {"name":"ucpub","kind":"cloud","account":"699166197771","region":"us-east-2",
  "reader_role_arn":"arn:aws:iam::699166197771:role/clavesa-ui-reader-ucpub","bucket":"ucpub-clavesa"}
]`))
	if err != nil {
		t.Fatal(err)
	}
	op := &fakeOpener{ws: &Workspace{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })}}
	s := NewServer(reg, service.HostedWellKnown{}, newMemStore(), op)
	open := func(groups, path string) Entry {
		op.opened = nil
		r := httptest.NewRequest(http.MethodGet, path, nil)
		r = r.WithContext(withViewer(r.Context(), viewerFromClaims(map[string]string{"cognito:groups": groups})))
		s.ServeHTTP(httptest.NewRecorder(), r)
		if len(op.opened) != 1 {
			t.Fatalf("%s %s: opened %d time(s)", groups, path, len(op.opened))
		}
		return op.opened[0]
	}
	if e := open("[all]", "/w/smoke2/api/pipelines"); !e.Admin || e.roleARN() != e.AdminRoleARN || e.viewName() != "smoke2--admin" {
		t.Errorf("admin opened %+v", e)
	}
	if e := open("[ws-smoke2]", "/w/smoke2/api/pipelines"); e.Admin || !strings.Contains(e.roleARN(), "reader") {
		t.Errorf("viewer opened %+v", e)
	}
	if e := open("[all]", "/w/ucpub/api/pipelines"); e.Admin {
		t.Errorf("no admin role, yet admin view: %+v", e)
	}
	if _, err := ParseRegistry([]byte(`[{"name":"x--admin","kind":"local"}]`)); err == nil {
		t.Error("a name ending in --admin was accepted")
	}
}
