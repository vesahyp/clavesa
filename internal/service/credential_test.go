package service

import (
	"errors"
	"os"
	"strings"
	"testing"
)

func TestCredentialAddListGetDelete(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	svc := New(ws)
	stored, err := svc.AddCredential(CredentialSpec{
		Name: "stripe", Kind: "header",
		HeaderName: "Authorization", ValuePrefix: "Bearer ",
		Secret: "env:STRIPE_KEY",
	})
	if err != nil {
		t.Fatalf("AddCredential: %v", err)
	}
	if stored.SecretBackend() != "env" {
		t.Errorf("backend = %q, want env", stored.SecretBackend())
	}
	list, err := svc.ListCredentials()
	if err != nil || len(list) != 1 {
		t.Fatalf("ListCredentials = %#v / %v", list, err)
	}
	if err := svc.DeleteCredential("stripe", false); err != nil {
		t.Fatalf("DeleteCredential: %v", err)
	}
	if _, err := svc.GetCredential("stripe"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("GetCredential after delete = %v, want os.ErrNotExist", err)
	}
}

func TestDeleteCredentialRefusesWhenInUse(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	svc := New(ws)
	if _, err := svc.AddCredential(CredentialSpec{
		Name: "stripe", Kind: "header",
		HeaderName: "Authorization", Secret: "env:STRIPE_KEY",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.AddSource(SourceSpec{
		Name: "events", Kind: "http",
		URL: "https://api.stripe.com/v1/events", Format: "json",
		Credentials: "stripe",
	}); err != nil {
		t.Fatalf("AddSource with credentials ref: %v", err)
	}
	err := svc.DeleteCredential("stripe", false)
	var inUse *ErrCredentialInUse
	if !errors.As(err, &inUse) {
		t.Fatalf("DeleteCredential = %v, want *ErrCredentialInUse", err)
	}
	if len(inUse.Usages) != 1 || inUse.Usages[0].SourceName != "events" {
		t.Errorf("Usages = %#v", inUse.Usages)
	}
	// --force overrides.
	if err := svc.DeleteCredential("stripe", true); err != nil {
		t.Errorf("DeleteCredential(force=true) = %v", err)
	}
}

func TestDeleteUnregisteredErrorsAreClean(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	svc := New(ws)
	cErr := svc.DeleteCredential("ghost", false)
	if cErr == nil {
		t.Fatal("expected error deleting unknown credential")
	}
	// Must satisfy errors.Is(os.ErrNotExist) so HTTP layer can 404.
	if !errors.Is(cErr, os.ErrNotExist) {
		t.Errorf("DeleteCredential(ghost) should satisfy errors.Is(os.ErrNotExist); got %v", cErr)
	}
	// Message must NOT leak the filesystem path.
	if strings.Contains(cErr.Error(), "/") || strings.Contains(cErr.Error(), "file does not exist") {
		t.Errorf("DeleteCredential(ghost) leaks filesystem internals: %q", cErr.Error())
	}
	sErr := svc.DeleteSource("ghost", false)
	if !errors.Is(sErr, os.ErrNotExist) {
		t.Errorf("DeleteSource(ghost) should satisfy errors.Is(os.ErrNotExist); got %v", sErr)
	}
	if strings.Contains(sErr.Error(), "/") || strings.Contains(sErr.Error(), "file does not exist") {
		t.Errorf("DeleteSource(ghost) leaks filesystem internals: %q", sErr.Error())
	}
}

func TestAddSourceRejectsUnknownCredential(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	svc := New(ws)
	_, err := svc.AddSource(SourceSpec{
		Name: "events", Kind: "http",
		URL: "https://api.stripe.com/v1/events", Format: "json",
		Credentials: "ghost",
	})
	if err == nil || !errors.Is(err, ErrCredentialNotFound) {
		t.Errorf("AddSource with unknown credential = %v, want ErrCredentialNotFound", err)
	}
	if !strings.Contains(err.Error(), "ghost") {
		t.Errorf("error should name the missing credential: %v", err)
	}
}
