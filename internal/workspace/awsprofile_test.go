package workspace

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestLoadAWSProfile(t *testing.T) {
	tests := []struct {
		name    string
		content string // "" = don't write the file
		want    string
	}{
		{"absent file → empty", "", ""},
		{"explicit profile", `{"profile":"webbaa"}`, "webbaa"},
		{"empty profile → empty", `{"profile":""}`, ""},
		{"whitespace trimmed", `{"profile":"  webbaa  "}`, "webbaa"},
		{"malformed json → empty", `{not json`, ""},
		{"empty object → empty", `{}`, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			if tt.content != "" {
				if err := os.MkdirAll(filepath.Join(root, ".clavesa"), 0o755); err != nil {
					t.Fatalf("mkdir: %v", err)
				}
				if err := os.WriteFile(AWSProfileFilePath(root), []byte(tt.content), 0o644); err != nil {
					t.Fatalf("write: %v", err)
				}
			}
			if got := LoadAWSProfile(root); got != tt.want {
				t.Errorf("LoadAWSProfile = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestWriteAWSProfileRoundTrip — Write then Load returns the same value;
// .clavesa/ is created if absent.
func TestWriteAWSProfileRoundTrip(t *testing.T) {
	root := t.TempDir()
	if err := WriteAWSProfile(root, "webbaa"); err != nil {
		t.Fatalf("WriteAWSProfile: %v", err)
	}
	if got := LoadAWSProfile(root); got != "webbaa" {
		t.Errorf("after write: LoadAWSProfile = %q, want webbaa", got)
	}
	// Clearing the override writes an empty value.
	if err := WriteAWSProfile(root, ""); err != nil {
		t.Fatalf("WriteAWSProfile clear: %v", err)
	}
	if got := LoadAWSProfile(root); got != "" {
		t.Errorf("after clear: LoadAWSProfile = %q, want empty", got)
	}
}

// TestListAWSProfiles — parses `[profile X]` from a config file and bare
// `[X]` from a credentials file, unions and sorts them.
func TestListAWSProfiles(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config")
	credsPath := filepath.Join(dir, "credentials")

	config := "[default]\nregion = eu-north-1\n\n[profile webbaa]\nregion = eu-north-1\n\n[profile personal]\n"
	creds := "[default]\naws_access_key_id = x\n\n[frosmo]\naws_access_key_id = y\n"
	if err := os.WriteFile(configPath, []byte(config), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if err := os.WriteFile(credsPath, []byte(creds), 0o644); err != nil {
		t.Fatalf("write creds: %v", err)
	}

	t.Setenv("AWS_CONFIG_FILE", configPath)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", credsPath)

	got := ListAWSProfiles()
	want := []string{"default", "frosmo", "personal", "webbaa"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ListAWSProfiles = %v, want %v", got, want)
	}
}

// TestListAWSProfilesNoFiles — a host with no AWS config yields an empty
// slice, not an error.
func TestListAWSProfilesNoFiles(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(dir, "nope-config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(dir, "nope-creds"))
	if got := ListAWSProfiles(); len(got) != 0 {
		t.Errorf("ListAWSProfiles = %v, want empty", got)
	}
}
