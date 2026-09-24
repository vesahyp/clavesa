package workspace

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// remoteStateCacheTTL bounds how long a fetched S3 state object is reused
// before ReadStackState fetches it again. The UI polls these readers per
// HTTP request (dashboard, run status, pipeline listing), so without a
// cache a busy page issues one S3 GetObject per reader per poll tick. The
// tfstate only changes on deploy, so a short TTL trades a little
// staleness for far fewer round trips.
const remoteStateCacheTTL = 30 * time.Second

type stateCacheEntry struct {
	data    []byte
	fetched time.Time
}

var (
	stateCacheMu sync.Mutex
	stateCache   = map[string]stateCacheEntry{}
)

// getRemoteState is the seam production code and tests dispatch remote
// state fetches through — same style as preview.SetRunnerForTest. The
// production implementation (getRemoteStateS3) hits S3; tests swap it via
// SetStateGetterForTest to avoid a live AWS call. A missing key is not an
// error: implementations return (nil, nil) for "not found".
var getRemoteState func(ctx context.Context, bucket, region, key string) ([]byte, error) = getRemoteStateS3

// SetStateGetterForTest swaps the remote-state getter and returns a
// function that restores the previous value. Also clears the in-process
// cache on both swap and restore so one test's cached reads never leak
// into the next. Use only from tests.
func SetStateGetterForTest(fn func(ctx context.Context, bucket, region, key string) ([]byte, error)) func() {
	prev := getRemoteState
	getRemoteState = fn
	clearStateCache()
	return func() {
		getRemoteState = prev
		clearStateCache()
	}
}

func clearStateCache() {
	stateCacheMu.Lock()
	defer stateCacheMu.Unlock()
	stateCache = map[string]stateCacheEntry{}
}

// ReadStackState is the one function every terraform-state read in clavesa
// goes through (ADR-025, "Read side: one function"). It returns the raw
// state JSON for the stack rooted at stackDir, or (nil, nil) when no state
// exists yet — "not deployed" is not an error, callers decide what that
// means.
//
// workspaceRoot locates the manifest that decides local vs remote.
// stackDir is the directory whose state is being read: either
// workspaceRoot itself (the workspace stack) or a pipeline directory
// under it. A legacy workspace with no clavesa.json, or one whose
// manifest carries no `backend`, reads local state exactly as before this
// function existed: <stackDir>/terraform.tfstate off disk.
//
// A backend-configured workspace instead fetches the S3 object at the
// stack's state key (ADR-025 "State keys": the workspace key when
// stackDir is workspaceRoot, the pipeline key otherwise) from
// backend.Bucket in backend.Region, over the default AWS credential
// chain — the same chain `deploy` already relies on (AWS_PROFILE /
// AWS_REGION / ~/.aws). A direct GetObject is used, not `terraform state
// pull`: no `terraform init` needed, and the key layout is clavesa's own.
// Remote fetches are cached for remoteStateCacheTTL, keyed on
// bucket+key — local reads are not cached.
func ReadStackState(ctx context.Context, workspaceRoot, stackDir string) ([]byte, error) {
	m, err := Load(workspaceRoot)
	if err != nil {
		return nil, err
	}
	if m == nil || m.Backend == nil {
		return readLocalStackState(stackDir)
	}

	key := stateKeyFor(m, workspaceRoot, stackDir)
	cacheKey := m.Backend.Bucket + "/" + key
	if data, ok := cachedState(cacheKey); ok {
		return data, nil
	}
	data, err := getRemoteState(ctx, m.Backend.Bucket, m.Backend.Region, key)
	if err != nil {
		return nil, err
	}
	cacheState(cacheKey, data)
	return data, nil
}

// readLocalStackState reads <stackDir>/terraform.tfstate. Absence is
// "not deployed" (nil, nil), not an error — every other read/parse
// failure is returned to the caller.
func readLocalStackState(stackDir string) ([]byte, error) {
	data, err := os.ReadFile(filepath.Join(stackDir, "terraform.tfstate"))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return data, nil
}

// stateKeyFor picks the workspace state key when stackDir names the
// workspace root itself (compared as cleaned absolute paths, so a
// relative stackDir resolves the same as workspaceRoot) and the pipeline
// state key otherwise (ADR-025 "State keys").
func stateKeyFor(m *Manifest, workspaceRoot, stackDir string) string {
	wsAbs, wsErr := filepath.Abs(workspaceRoot)
	stackAbs, stackErr := filepath.Abs(stackDir)
	if wsErr == nil && stackErr == nil && wsAbs == stackAbs {
		return m.Backend.WorkspaceStateKey(m.Name)
	}
	return m.Backend.PipelineStateKey(m.Name, filepath.Base(stackDir))
}

func cachedState(key string) ([]byte, bool) {
	stateCacheMu.Lock()
	defer stateCacheMu.Unlock()
	entry, ok := stateCache[key]
	if !ok || time.Since(entry.fetched) > remoteStateCacheTTL {
		return nil, false
	}
	return entry.data, true
}

func cacheState(key string, data []byte) {
	stateCacheMu.Lock()
	defer stateCacheMu.Unlock()
	stateCache[key] = stateCacheEntry{data: data, fetched: time.Now()}
}

// getRemoteStateS3 is the production remote-state getter: a plain S3
// GetObject using the default AWS credential chain, scoped to the
// backend's region. A missing key (S3's NoSuchKey) is "not found", not an
// error.
func getRemoteStateS3(ctx context.Context, bucket, region, key string) ([]byte, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	client := s3.NewFromConfig(cfg)
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNoSuchKey(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get state s3://%s/%s: %w", bucket, key, err)
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

// isNoSuchKey matches the SDK v2 NoSuchKey shape the same way
// internal/delta/s3fs does: a typed error code first, falling back to a
// message match for the untyped-404 case.
func isNoSuchKey(err error) bool {
	var nf interface{ ErrorCode() string }
	if errors.As(err, &nf) {
		switch nf.ErrorCode() {
		case "NoSuchKey", "NotFound", "404":
			return true
		}
	}
	return strings.Contains(err.Error(), "NoSuchKey")
}

// PipelineBucket reads the workspace stack's state and returns the
// `pipeline_bucket` output, or "" if the workspace hasn't been deployed
// yet (no state, no outputs, or apply not yet run). Callers use this to
// auto-derive cloud-side defaults like ATHENA_OUTPUT_BUCKET — never to
// drive deployment behavior, since the state may be stale. Goes through
// ReadStackState (ADR-025), so it works the same whether the workspace is
// local- or remote-backed; the 10s timeout bounds a remote fetch without
// threading a caller context through every one of PipelineBucket's many
// call sites.
func PipelineBucket(workspaceRoot string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	data, err := ReadStackState(ctx, workspaceRoot, workspaceRoot)
	if err != nil || data == nil {
		return ""
	}
	return tfstateOutput(data, "pipeline_bucket")
}

// tfstateOutput pulls a named string output from state JSON. Returns ""
// on any parse/missing error — caller decides whether that's an empty
// case or a hard error. Format:
//
//	{ "outputs": { "<name>": { "value": <…>, "type": "string" } } }
func tfstateOutput(data []byte, name string) string {
	var s struct {
		Outputs map[string]struct {
			Value any `json:"value"`
		} `json:"outputs"`
	}
	if err := json.Unmarshal(data, &s); err != nil {
		return ""
	}
	out, ok := s.Outputs[name]
	if !ok {
		return ""
	}
	if str, ok := out.Value.(string); ok {
		return str
	}
	return ""
}
