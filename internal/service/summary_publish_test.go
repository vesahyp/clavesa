package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestFetchHostedWellKnown(t *testing.T) {
	want := HostedWellKnown{
		ReaderPrincipal:  hostedTestPrincipal,
		EventBusARN:      hostedTestBus,
		PublisherRoleARN: "arn:aws:iam::111122223333:role/clavesa-ui-publisher",
		DataBucket:       "clavesa-ui-data-111122223333",
		Region:           "eu-north-1",
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case HostedWellKnownPath:
			_ = json.NewEncoder(w).Encode(want)
		case "/empty/.well-known/clavesa.json":
			_, _ = w.Write([]byte(`{}`))
		default:
			http.Error(w, "<html>nope</html>", http.StatusNotFound)
		}
	}))
	defer srv.Close()

	got, err := fetchHostedWellKnown(context.Background(), srv.URL+"/")
	if err != nil || got != want {
		t.Fatalf("got %+v, %v", got, err)
	}
	if _, err := fetchHostedWellKnown(context.Background(), srv.URL+"/empty"); err == nil || !strings.Contains(err.Error(), "reader_principal") {
		t.Errorf("a document with no reader_principal: %v", err)
	}
	if _, err := fetchHostedWellKnown(context.Background(), srv.URL+"/missing"); err == nil || !strings.Contains(err.Error(), "HTTP 404") {
		t.Errorf("404: %v", err)
	}
}

type fakePutter struct {
	bucket, key string
	body        []byte
	err         error
}

func (f *fakePutter) PutSummary(_ context.Context, bucket, key string, body []byte) error {
	f.bucket, f.key, f.body = bucket, key, body
	return f.err
}

// TestPublishSummary: --publish writes the summary JSON to
// summaries/<name>.json in the well-known data bucket, through the
// publisher role the well-known document names (fake S3).
func TestPublishSummary(t *testing.T) {
	ws := hostedWorkspace(t)
	svc := New(ws)
	sum := &WorkspaceSummary{Name: "smoke2", Status: SummaryOK, GeneratedAt: "2026-11-15T12:00:00Z"}

	if _, err := svc.PublishSummary(context.Background(), sum); err == nil || !strings.Contains(err.Error(), "not hosted") {
		t.Errorf("not hosted: %v", err)
	}

	wk := HostedWellKnown{ReaderPrincipal: hostedTestPrincipal, PublisherRoleARN: "arn:aws:iam::111122223333:role/clavesa-ui-publisher",
		DataBucket: "clavesa-ui-data-111122223333", Region: "eu-north-1"}
	stubWellKnown(t, wk, nil)
	if _, err := svc.EnableHosted(context.Background(), HostedInput{URL: "https://ui.clavesa.dev"}); err != nil {
		t.Fatal(err)
	}
	put := &fakePutter{}
	var gotRole, gotSession string
	prev := newSummaryPutter
	newSummaryPutter = func(_ context.Context, root, session string, w HostedWellKnown) (SummaryPutter, error) {
		gotRole, gotSession = w.PublisherRoleARN, session
		return put, nil
	}
	t.Cleanup(func() { newSummaryPutter = prev })

	res, err := svc.PublishSummary(context.Background(), sum)
	if err != nil {
		t.Fatal(err)
	}
	if res.URI != "s3://clavesa-ui-data-111122223333/summaries/smoke2.json" || put.key != "summaries/smoke2.json" || put.bucket != wk.DataBucket {
		t.Errorf("published %+v to s3://%s/%s", res, put.bucket, put.key)
	}
	if gotRole != wk.PublisherRoleARN || gotSession != "clavesa-publish-smoke2" {
		t.Errorf("role %q session %q", gotRole, gotSession)
	}
	var back WorkspaceSummary
	if err := json.Unmarshal(put.body, &back); err != nil || back.Name != "smoke2" || back.GeneratedAt != sum.GeneratedAt {
		t.Errorf("body %s: %v", put.body, err)
	}

	// S3 refusing the write (a name the role may not write) is an error.
	put.err = errors.New("AccessDenied")
	if _, err := svc.PublishSummary(context.Background(), sum); err == nil || !strings.Contains(err.Error(), "AccessDenied") {
		t.Errorf("refused put: %v", err)
	}
	// A hosted UI that names no publisher role.
	stubWellKnown(t, HostedWellKnown{ReaderPrincipal: hostedTestPrincipal}, nil)
	if _, err := svc.PublishSummary(context.Background(), sum); err == nil || !strings.Contains(err.Error(), "publisher role") {
		t.Errorf("no publisher: %v", err)
	}
}
