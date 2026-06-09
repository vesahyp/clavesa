package service

import "testing"

// TestRunnerResponseStatus pins how the backfill paths read the runner
// Lambda's response envelope. A present non-ok "status" is fatal; an
// absent or unparseable status collapses to "" (treated as ok by callers).
func TestRunnerResponseStatus(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		want    string
	}{
		{"ok status", `{"status":"ok","rows_written":42}`, "ok"},
		{"skipped status", `{"status":"skipped","reason":"no new partitions"}`, "skipped"},
		{"failed status", `{"status":"failed","error_msg":"boom"}`, "failed"},
		{"no status key", `{"rows_written":42}`, ""},
		{"empty payload", ``, ""},
		{"unparseable", `not json`, ""},
		{"non-string status", `{"status":7}`, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := runnerResponseStatus([]byte(c.payload)); got != c.want {
				t.Errorf("runnerResponseStatus(%q) = %q, want %q", c.payload, got, c.want)
			}
		})
	}
}

// TestRunnerResponseMessage pins the reason → error_msg → raw-payload
// fallback used to surface a runner failure to the user.
func TestRunnerResponseMessage(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		want    string
	}{
		{"reason preferred", `{"status":"skipped","reason":"no new partitions","error_msg":"x"}`, "no new partitions"},
		{"error_msg fallback", `{"status":"failed","error_msg":"boom"}`, "boom"},
		{"raw payload fallback", `{"status":"failed"}`, `{"status":"failed"}`},
		{"unparseable returns raw", `weird`, "weird"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := runnerResponseMessage([]byte(c.payload)); got != c.want {
				t.Errorf("runnerResponseMessage(%q) = %q, want %q", c.payload, got, c.want)
			}
		})
	}
}
