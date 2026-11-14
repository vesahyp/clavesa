package bundle

import "testing"

func TestAWSCron(t *testing.T) {
	for _, c := range []struct{ in, want, err string }{
		{"0 30 4 * * ?", "cron(30 4 * * ? *)", ""},
		{"0 0 6 ? * MON-FRI", "cron(0 6 ? * MON-FRI *)", ""},
		{"0 15 10 L * ? 2027", "cron(15 10 L * ? 2027)", ""},
		{"0 0 12 * * *", "cron(0 12 * * ? *)", ""},
		{"30 0 12 * * ?", "", "the seconds field is 30"},
		{"0 0 12 ? * ?", "", "both ?"},
		{"0 0 12 1 * 2", "", "set ? in day-of-month or day-of-week"},
		{"0 0 12 * *", "", "want 6 or 7 fields"},
	} {
		got, err := AWSCron(c.in)
		if c.err != "" {
			if err == nil || !containsStr(err.Error(), c.err) {
				t.Errorf("%q: want error %q, got %v", c.in, c.err, err)
			}
			continue
		}
		if err != nil || got != c.want {
			t.Errorf("%q: got %q %v, want %q", c.in, got, err, c.want)
		}
	}
	for _, c := range []struct {
		n    int
		unit string
		want string
	}{{1, "HOURS", "rate(1 hour)"}, {4, "HOURS", "rate(4 hours)"}, {1, "DAYS", "rate(1 day)"}, {2, "WEEKS", "rate(14 days)"}} {
		if got, err := AWSRate(c.n, c.unit); err != nil || got != c.want {
			t.Errorf("%d %s: %q %v", c.n, c.unit, got, err)
		}
	}
	if _, err := AWSRate(1, "MINUTES"); err == nil {
		t.Error("MINUTES is not a Databricks unit")
	}
}

func containsStr(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
