package workspace

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// awsProfileFile is the per-workspace AWS-profile file under
// .clavesa/. Like environment.json it is a per-developer, gitignored
// preference — the AWS profile the local `clavesa ui` server (and
// the runs it dispatches) operate as. Absent means "use whatever the
// shell / default credential chain resolves".
const awsProfileFile = "aws-profile.json"

type awsProfileDoc struct {
	Profile string `json:"profile"`
}

// AWSProfileFilePath returns the path of the AWS-profile file for the
// workspace rooted at root.
func AWSProfileFilePath(root string) string {
	return filepath.Join(root, ".clavesa", awsProfileFile)
}

// LoadAWSProfile reports the workspace's persisted AWS profile. An
// absent file, an unreadable file, or malformed JSON all resolve to ""
// — meaning "no per-workspace override; fall back to the ambient
// AWS_PROFILE / default credential chain". Pure read.
func LoadAWSProfile(root string) string {
	data, err := os.ReadFile(AWSProfileFilePath(root))
	if err != nil {
		return ""
	}
	var doc awsProfileDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		return ""
	}
	return strings.TrimSpace(doc.Profile)
}

// WriteAWSProfile persists the workspace AWS profile, creating
// .clavesa/ if needed. An empty profile clears the override (the
// file is written with an empty value rather than deleted, so the
// choice "explicitly none" is distinguishable in tooling). Written by
// `workspace use --profile` and the HTTP aws-profile endpoint.
func WriteAWSProfile(root, profile string) error {
	dir := filepath.Join(root, ".clavesa")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(awsProfileDoc{Profile: strings.TrimSpace(profile)}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, awsProfileFile), append(data, '\n'), 0o644)
}

// ListAWSProfiles returns the AWS profile names configured on this host,
// parsed from ~/.aws/config and ~/.aws/credentials (or the locations
// AWS_CONFIG_FILE / AWS_SHARED_CREDENTIALS_FILE point at). The result is
// sorted and de-duplicated. Returns an empty slice when neither file
// exists — a host with no AWS config at all.
func ListAWSProfiles() []string {
	seen := map[string]struct{}{}

	configPath := os.Getenv("AWS_CONFIG_FILE")
	if configPath == "" {
		if home, err := os.UserHomeDir(); err == nil {
			configPath = filepath.Join(home, ".aws", "config")
		}
	}
	credsPath := os.Getenv("AWS_SHARED_CREDENTIALS_FILE")
	if credsPath == "" {
		if home, err := os.UserHomeDir(); err == nil {
			credsPath = filepath.Join(home, ".aws", "credentials")
		}
	}

	// config file: sections are `[default]` and `[profile <name>]`.
	for _, name := range iniSections(configPath) {
		if name == "default" {
			seen[name] = struct{}{}
			continue
		}
		if rest, ok := strings.CutPrefix(name, "profile "); ok {
			if n := strings.TrimSpace(rest); n != "" {
				seen[n] = struct{}{}
			}
		}
	}
	// credentials file: sections are bare `[<name>]`.
	for _, name := range iniSections(credsPath) {
		seen[name] = struct{}{}
	}

	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// iniSections returns the section header names ("foo" from a `[foo]`
// line) of the INI-style file at path. A missing or unreadable file
// yields an empty slice — the caller treats that as "no profiles here".
func iniSections(path string) []string {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()

	var out []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if len(line) >= 2 && strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			name := strings.TrimSpace(line[1 : len(line)-1])
			if name != "" {
				out = append(out, name)
			}
		}
	}
	return out
}
