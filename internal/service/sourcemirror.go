// Local source mirror (ADR-026, GH #91). On a local-warehouse `pipeline run`,
// non-partitioned s3 registry sources are synced into
// `<workspace>/.clavesa/cache/sources/<name>/` and the runner reads the mirror
// through a `kind=path` descriptor instead of scanning bucket/prefix over the
// network on every Spark action. Steady-state cost per run is one
// ListObjectsV2 walk plus the day's new files; repeated in-run scans hit local
// disk. The cache dir is the disposable, gitignored, delete-anytime area — a
// deleted mirror simply re-syncs on the next run.
package service

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/vesahyp/clavesa/internal/sources"
)

// mirrorS3Client is the subset of the AWS SDK v2 S3 client the source mirror
// uses. Narrow on purpose — keeps the test stub small and the dependency
// surface obvious (mirrors resetS3API / internal/delta/s3fs.S3API). The
// service's lazily-built dataquery.S3Client satisfies it.
type mirrorS3Client interface {
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// mirrorSummary is what one syncSourceMirror pass did, for the run log line.
type mirrorSummary struct {
	Downloaded int   // objects fetched this pass (new or changed)
	Deleted    int   // local files removed because their key vanished remotely
	Files      int   // total objects in the mirror after the pass
	Bytes      int64 // total mirror size in bytes after the pass
}

// mirrorDownloadWorkers bounds the concurrent GetObject downloads. Modest on
// purpose — the win is not re-downloading at all, not download parallelism.
const mirrorDownloadWorkers = 8

// sourceMirrorEnabled reports whether the ADR-026 local source mirror is
// active. CLAVESA_SOURCE_MIRROR=off restores the direct-S3 descriptor
// entirely, for sources too large to spend local disk on.
func sourceMirrorEnabled() bool {
	return os.Getenv("CLAVESA_SOURCE_MIRROR") != "off"
}

// mirrorClient returns the host-side S3 client the mirror sync uses. Tests
// inject a fake via s.mirrorS3; production resolves the default AWS credential
// chain lazily through ensureS3Client (the same client preview's s3 listing
// uses).
func (s *Service) mirrorClient() (mirrorS3Client, error) {
	if s.mirrorS3 != nil {
		return s.mirrorS3, nil
	}
	if err := s.ensureS3Client(); err != nil {
		return nil, err
	}
	return s.s3Client, nil
}

// mirrorSourceDescriptor syncs the source's mirror and returns the
// `kind=path` input descriptor pointing at it — the shape the runner and the
// input-mount collector already handle (inputLocalPath). Called from
// buildInputs for non-partitioned, non-credentialed s3 sources on the
// local-warehouse run paths. A sync failure fails the run: no silent fallback
// to a stale mirror — stale input data indistinguishable from fresh is worse
// than a failed run (ADR-026).
func (s *Service) mirrorSourceDescriptor(ctx context.Context, spec sources.Spec) (map[string]any, error) {
	client, err := s.mirrorClient()
	if err != nil {
		return nil, fmt.Errorf("mirror sync: %w", err)
	}
	destDir := filepath.Join(s.workspace, ".clavesa", "cache", "sources", spec.Name)
	sum, err := syncSourceMirror(ctx, client, spec.Bucket, spec.Prefix, destDir)
	if err != nil {
		return nil, fmt.Errorf("mirror sync: %w", err)
	}
	fmt.Printf("source %s: mirrored %d new, %d deleted (%d files, %s) → .clavesa/cache/sources/%s/\n",
		spec.Name, sum.Downloaded, sum.Deleted, sum.Files, mirrorFormatBytes(sum.Bytes), spec.Name)
	descriptor := map[string]any{
		"kind":   "path",
		"path":   destDir,
		"format": spec.Format,
	}
	if len(spec.ReadOptions) > 0 {
		descriptor["read_options"] = spec.ReadOptions
	}
	return descriptor, nil
}

// mirrorObject is one remote object's change-detection state.
type mirrorObject struct {
	size         int64
	lastModified time.Time
}

// syncSourceMirror makes destDir an exact local copy of s3://bucket/prefix:
//
//   - one ListObjectsV2 walk of the prefix per call;
//   - keys that are new or changed (size differs, or S3 LastModified differs
//     from the local file's mtime) are downloaded; after a download the local
//     mtime is set to the S3 LastModified, making the comparison cheap and
//     idempotent;
//   - local files whose key no longer exists remotely are deleted (a mirror,
//     not an accumulator — S3 lifecycle expiry must propagate);
//   - keys nest into subdirectories on "/".
func syncSourceMirror(ctx context.Context, client mirrorS3Client, bucket, prefix, destDir string) (mirrorSummary, error) {
	var sum mirrorSummary

	// Full listing first — it is also the delete-pass truth.
	remote := map[string]mirrorObject{} // "/"-separated prefix-relative key
	var continuation *string
	for {
		page, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuation,
		})
		if err != nil {
			return sum, fmt.Errorf("list s3://%s/%s: %w", bucket, prefix, err)
		}
		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			rel := strings.TrimPrefix(key, prefix)
			if rel == "" || strings.HasSuffix(rel, "/") {
				// The prefix itself / zero-byte directory markers.
				continue
			}
			var lm time.Time
			if obj.LastModified != nil {
				lm = *obj.LastModified
			}
			remote[rel] = mirrorObject{size: aws.ToInt64(obj.Size), lastModified: lm}
		}
		if page.IsTruncated == nil || !*page.IsTruncated {
			break
		}
		continuation = page.NextContinuationToken
	}

	if err := os.MkdirAll(destDir, 0o755); err != nil {
		return sum, fmt.Errorf("create mirror dir: %w", err)
	}

	// Change detection against the local tree.
	var toFetch []string
	for rel, obj := range remote {
		sum.Files++
		sum.Bytes += obj.size
		local, err := mirrorLocalPath(destDir, rel)
		if err != nil {
			return sum, err
		}
		st, err := os.Stat(local)
		if err != nil || st.Size() != obj.size || !st.ModTime().Equal(obj.lastModified) {
			toFetch = append(toFetch, rel)
		}
	}
	sort.Strings(toFetch)

	// Bounded-concurrency downloads. First error wins; remaining workers
	// finish their in-flight object and the rest are skipped.
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	sem := make(chan struct{}, mirrorDownloadWorkers)
	for _, rel := range toFetch {
		wg.Add(1)
		go func(rel string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			mu.Lock()
			skip := firstErr != nil
			mu.Unlock()
			if skip {
				return
			}
			err := downloadMirrorObject(ctx, client, bucket, prefix, rel, destDir, remote[rel].lastModified)
			mu.Lock()
			if err != nil && firstErr == nil {
				firstErr = err
			}
			if err == nil {
				sum.Downloaded++
			}
			mu.Unlock()
		}(rel)
	}
	wg.Wait()
	if firstErr != nil {
		return sum, firstErr
	}

	// Delete pass: drop local files whose key vanished remotely.
	err := filepath.WalkDir(destDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, rerr := filepath.Rel(destDir, path)
		if rerr != nil {
			return rerr
		}
		if _, ok := remote[filepath.ToSlash(rel)]; ok {
			return nil
		}
		if rmErr := os.Remove(path); rmErr != nil {
			return rmErr
		}
		sum.Deleted++
		return nil
	})
	if err != nil {
		return sum, fmt.Errorf("prune mirror: %w", err)
	}
	return sum, nil
}

// mirrorLocalPath maps a "/"-separated prefix-relative key onto a path under
// destDir, refusing keys that would escape it (S3 keys may contain "..").
func mirrorLocalPath(destDir, rel string) (string, error) {
	local := filepath.Join(destDir, filepath.FromSlash(rel))
	if local != destDir && !strings.HasPrefix(local, destDir+string(filepath.Separator)) {
		return "", fmt.Errorf("key %q escapes the mirror directory", rel)
	}
	return local, nil
}

// downloadMirrorObject fetches one object into the mirror: GetObject → temp
// file in the destination directory → rename → mtime set to the S3
// LastModified (the change-detection contract).
func downloadMirrorObject(ctx context.Context, client mirrorS3Client, bucket, prefix, rel, destDir string, lastModified time.Time) error {
	local, err := mirrorLocalPath(destDir, rel)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(local), 0o755); err != nil {
		return fmt.Errorf("create mirror subdir for %s: %w", rel, err)
	}
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(prefix + rel),
	})
	if err != nil {
		return fmt.Errorf("get s3://%s/%s%s: %w", bucket, prefix, rel, err)
	}
	defer out.Body.Close()
	tmp, err := os.CreateTemp(filepath.Dir(local), ".clavesa-mirror-*")
	if err != nil {
		return fmt.Errorf("create temp file for %s: %w", rel, err)
	}
	if _, err := io.Copy(tmp, out.Body); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return fmt.Errorf("download s3://%s/%s%s: %w", bucket, prefix, rel, err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return fmt.Errorf("write %s: %w", rel, err)
	}
	if err := os.Rename(tmp.Name(), local); err != nil {
		os.Remove(tmp.Name())
		return fmt.Errorf("place %s: %w", rel, err)
	}
	if err := os.Chtimes(local, lastModified, lastModified); err != nil {
		return fmt.Errorf("set mtime on %s: %w", rel, err)
	}
	return nil
}

// mirrorFormatBytes renders a byte count in binary units (B/KB/MB/GB/…) with
// one decimal place above the byte range. Local twin of the CLI's formatBytes
// (internal/cli imports this package, so the reverse import would cycle).
func mirrorFormatBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), "KMGTPE"[exp])
}
