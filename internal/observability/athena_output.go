package observability

import (
	"fmt"
	"os"
	"regexp"
	"strings"
)

// AthenaOutputEnv names the setting that moves every Athena query result
// clavesa writes to a prefix of the user's choice:
//
//	CLAVESA_ATHENA_OUTPUT=s3://web-traffic-clavesa/athena-results/viewer/
//
// A viewer whose IAM role may read only that prefix then never reads
// another user's past results in the shared athena-results/ prefix.
// Athena reads the results of a query (GetQueryResults) from the query's
// own output location, so the setting covers the reads too. Result reuse
// stays on: Athena reuses a result only for the same result configuration
// and only when the caller may read the earlier result, so a viewer
// reuses only its own results.
//
// The setting applies to every workspace of the process (also with
// clavesa ui --workspaces), so all of them must be able to write there.
// The role needs s3:PutObject, s3:GetObject, s3:ListBucket and
// s3:GetBucketLocation on that prefix.
//
// The hosted reader's workgroup (ADR-032) enforces its own location; a
// query through it fails while the setting is set (runAthenaQuery).
const AthenaOutputEnv = "CLAVESA_ATHENA_OUTPUT"

// bucketRE is an S3 bucket name.
var bucketRE = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)

// AthenaOutputSet reports whether $CLAVESA_ATHENA_OUTPUT is set.
func AthenaOutputSet() bool { return strings.TrimSpace(os.Getenv(AthenaOutputEnv)) != "" }

// AthenaOutputLocation returns the s3:// prefix for Athena query results:
// $CLAVESA_ATHENA_OUTPUT when set, else defaultLocation. The result
// always ends in "/". A setting that is not an s3://<bucket>/ URI is an
// error, so a typo never sends results somewhere unexpected.
func AthenaOutputLocation(defaultLocation string) (string, error) {
	v := strings.TrimSpace(os.Getenv(AthenaOutputEnv))
	if v == "" {
		return withSlash(defaultLocation), nil
	}
	rest, ok := strings.CutPrefix(v, "s3://")
	bucket, _, _ := strings.Cut(rest, "/")
	if !ok || !bucketRE.MatchString(bucket) || strings.ContainsAny(v, " ?#") {
		return "", fmt.Errorf("%s=%q: want an s3://<bucket>/<prefix>/ URI", AthenaOutputEnv, v)
	}
	return withSlash(v), nil
}

// DefaultAthenaOutput is the default results prefix,
// s3://<bucket>/athena-results/. bucket is a bare bucket name.
func DefaultAthenaOutput(bucket string) string {
	return "s3://" + bucket + "/athena-results/"
}

func withSlash(s string) string {
	if s != "" && !strings.HasSuffix(s, "/") {
		return s + "/"
	}
	return s
}
