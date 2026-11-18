package hosted

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
)

// Workspace kinds in the registry (ADR-032 section 2).
const (
	// KindCloud is a cloud-warehouse workspace: the Lambda reads it live
	// through its reader role and refreshes its summary on its events.
	KindCloud = "cloud"
	// KindLocal is a local-warehouse workspace: the Mac publishes its
	// summary through the publisher role; there is no live read.
	KindLocal = "local"
)

// Entry is one workspace of the registry, hosted/infra/workspaces.tf. It
// reaches the Lambda as the JSON array in the REGISTRY environment
// variable.
type Entry struct {
	Name string `json:"name"`
	Kind string `json:"kind"`
	// Account and Region are required for a cloud workspace and optional
	// for a local one ({name, kind:"local"} is a whole local entry).
	Account string `json:"account"`
	Region  string `json:"region"`
	// ReaderRoleARN and Bucket (the pipeline bucket) are set for a cloud
	// workspace only.
	ReaderRoleARN string `json:"reader_role_arn,omitempty"`
	Bucket        string `json:"bucket,omitempty"`
	// PersonalData: the workspace's raw data holds personal data, so the
	// hosted API refuses raw row reads of it (personal.go).
	PersonalData bool `json:"personal_data,omitempty"`
}

var (
	// entryNameRE is what a name must match to be served at /w/<name>/
	// and stored at summaries/<name>.json.
	entryNameRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)
	accountRE   = regexp.MustCompile(`^[0-9]{12}$`)
	roleARNRE   = regexp.MustCompile(`^arn:aws[a-z-]*:iam::[0-9]{12}:role/[A-Za-z0-9+=,.@_/-]+$`)
	bucketRE    = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)
)

// ParseRegistry decodes and checks the registry. An empty value is an
// empty registry. Entries come back sorted by name.
func ParseRegistry(data []byte) ([]Entry, error) {
	if len(data) == 0 {
		return []Entry{}, nil
	}
	var entries []Entry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("registry: %w", err)
	}
	seen := map[string]bool{}
	for i, e := range entries {
		if err := e.validate(); err != nil {
			return nil, fmt.Errorf("registry entry %d (%q): %w", i, e.Name, err)
		}
		if seen[e.Name] {
			return nil, fmt.Errorf("registry: workspace %q is listed twice", e.Name)
		}
		seen[e.Name] = true
	}
	if entries == nil {
		entries = []Entry{}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	return entries, nil
}

func (e Entry) validate() error {
	// "all" is the group that sees every workspace (ADR-035): a workspace
	// of that name could not be granted alone.
	if e.Name == GroupAll {
		return fmt.Errorf("workspace name %q is reserved: it is the viewer group that sees every workspace", e.Name)
	}
	if !entryNameRE.MatchString(e.Name) {
		return fmt.Errorf("name must match %s", entryNameRE)
	}
	// A local workspace lives on a Mac, not in an account: account and
	// region are optional there (its summary names its region).
	optional := e.Kind == KindLocal
	if !(optional && e.Account == "") && !accountRE.MatchString(e.Account) {
		return fmt.Errorf("account must be a 12-digit AWS account id")
	}
	if e.Region == "" && !optional {
		return fmt.Errorf("region is required")
	}
	switch e.Kind {
	case KindCloud:
		if !roleARNRE.MatchString(e.ReaderRoleARN) {
			return fmt.Errorf("a cloud workspace needs reader_role_arn (arn:aws:iam::<account>:role/clavesa-ui-reader-<name>)")
		}
		if !bucketRE.MatchString(e.Bucket) {
			return fmt.Errorf("a cloud workspace needs bucket, its pipeline bucket")
		}
	case KindLocal:
		if e.ReaderRoleARN != "" || e.Bucket != "" {
			return fmt.Errorf("a local workspace has no reader_role_arn or bucket")
		}
	default:
		return fmt.Errorf("kind must be %q or %q", KindCloud, KindLocal)
	}
	return nil
}
