package observability

// ParseFreshnessSLA converts an HCL "4h" / "30m" / "7d" / "120s" budget into
// seconds. Mirrors the validation regex on the Terraform variable. Returns
// (0, false) for malformed strings; callers treat that as "no SLA". The
// catalog's freshness chip and service.WorkspaceSummary both read
// `freshness_sla` through this one parser.
func ParseFreshnessSLA(s string) (int64, bool) {
	if len(s) < 2 {
		return 0, false
	}
	suffix := s[len(s)-1]
	numStr := s[:len(s)-1]
	if numStr == "" {
		return 0, false
	}
	var n int64
	for _, c := range numStr {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int64(c-'0')
	}
	switch suffix {
	case 's':
		return n, true
	case 'm':
		return n * 60, true
	case 'h':
		return n * 3600, true
	case 'd':
		return n * 86400, true
	}
	return 0, false
}
