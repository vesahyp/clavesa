// Package s3fs adapts an S3 bucket+prefix to the io/fs.FS interface so
// internal/delta.ReadCurrent can read a Delta transaction log directly
// from S3 the same way it reads one from disk.
//
// Scope. Only the operations delta.ReadCurrent uses are wired:
//   - ReadDir(".") — list the immediate children of the prefix
//   - Open(name) → file with Read+Close+Stat returning mtime
//
// Nested directories, ReadFile, Sub-FS, Glob, and Walk are unsupported
// (fs.ReadFile falls back to Open+ReadAll, which is how the delta reader
// fetches checkpoint parquet parts). The S3 listing is paged but uses a
// single iteration; a `_delta_log/` with more commit files than fit in one
// ListObjectsV2 page (1000 by default) is handled.
package s3fs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3API is the subset of the AWS SDK v2 S3 client this package uses.
// Narrow on purpose — keeps the test stub small and makes the
// dependency surface obvious.
type S3API interface {
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// FS is an fs.FS rooted at s3://Bucket/Prefix/. Prefix MUST end with a
// trailing slash unless empty — delta.ReadCurrent is rooted at
// `_delta_log/`, so callers typically construct with
// `New(client, bucket, "<key>/_delta_log/", ctx)`.
type FS struct {
	client S3API
	bucket string
	prefix string
	ctx    context.Context
}

// New builds an FS bound to a specific context (used for every S3
// roundtrip). The ctx allows callers to bound the catalog/snapshot read
// with the inbound HTTP request's deadline so a stuck S3 call doesn't
// dangle past the client timeout.
func New(ctx context.Context, client S3API, bucket, prefix string) *FS {
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return &FS{client: client, bucket: bucket, prefix: prefix, ctx: ctx}
}

// Open returns the object at Prefix/<name>. fs.ErrNotExist is returned
// for missing keys (S3's NoSuchKey) so delta.ReadCurrent can surface
// ErrNotDelta for missing-table cases.
func (f *FS) Open(name string) (fs.File, error) {
	if name == "." {
		// Synthesize a "directory" file so fs.ReadDir(fsys, ".") falls
		// through to ReadDir below — fs.ReadDir tries the ReadDirFS
		// interface first, so this branch is mostly belt-and-braces.
		return &s3Dir{f: f}, nil
	}
	key := f.prefix + name
	out, err := f.client.GetObject(f.ctx, &s3.GetObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
		}
		return nil, &fs.PathError{Op: "open", Path: name, Err: err}
	}
	var modTime time.Time
	if out.LastModified != nil {
		modTime = *out.LastModified
	}
	var size int64
	if out.ContentLength != nil {
		size = *out.ContentLength
	}
	return &s3File{
		name:    name,
		size:    size,
		modTime: modTime,
		body:    out.Body,
	}, nil
}

// ReadDir lists Prefix/<name>/. Only name == "." is supported — Delta's
// transaction-log walk is single-level. Pages through ListObjectsV2 if
// the prefix carries more than 1000 commit files.
func (f *FS) ReadDir(name string) ([]fs.DirEntry, error) {
	if name != "." {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: fs.ErrInvalid}
	}
	out := []fs.DirEntry{}
	var token *string
	for {
		resp, err := f.client.ListObjectsV2(f.ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(f.bucket),
			Prefix:            aws.String(f.prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, &fs.PathError{Op: "readdir", Path: name, Err: err}
		}
		for _, obj := range resp.Contents {
			key := aws.ToString(obj.Key)
			if !strings.HasPrefix(key, f.prefix) {
				continue
			}
			rel := key[len(f.prefix):]
			// Skip nested subdirectory entries — delta.ReadCurrent's
			// commit-file regex would skip them anyway, but elide here
			// to keep DirEntry counts honest.
			if strings.Contains(rel, "/") {
				continue
			}
			if rel == "" {
				continue
			}
			var size int64
			if obj.Size != nil {
				size = *obj.Size
			}
			var mtime time.Time
			if obj.LastModified != nil {
				mtime = *obj.LastModified
			}
			out = append(out, &s3DirEntry{
				name:    rel,
				size:    size,
				modTime: mtime,
			})
		}
		if resp.IsTruncated == nil || !*resp.IsTruncated {
			break
		}
		token = resp.NextContinuationToken
	}
	return out, nil
}

// Stat returns metadata for Prefix/<name>. delta.ReadCurrent's
// fs.Stat fallback uses this when commitInfo.timestamp is missing —
// surface S3's LastModified the same way os.Stat surfaces mtime.
func (f *FS) Stat(name string) (fs.FileInfo, error) {
	file, err := f.Open(name)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return file.Stat()
}

// isNotFound matches the SDK v2 NoSuchKey shape. The v2 client returns
// a `*types.NoSuchKey` for HeadObject/GetObject misses but the error
// chain can also carry an HTTP 404 with no typed wrapper, so we match
// by message as well — the same pattern used elsewhere in the codebase
// (internal/dataquery/source.go).
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	var nf interface{ ErrorCode() string }
	if errors.As(err, &nf) {
		code := nf.ErrorCode()
		if code == "NoSuchKey" || code == "NotFound" || code == "404" {
			return true
		}
	}
	msg := err.Error()
	return strings.Contains(msg, "NoSuchKey") || strings.Contains(msg, "status code: 404")
}

// ---------------------------------------------------------------------------
// fs.File / fs.DirEntry implementations
// ---------------------------------------------------------------------------

type s3File struct {
	name    string
	size    int64
	modTime time.Time
	body    io.ReadCloser
	// readErr captures a fault encountered during Read so subsequent
	// reads return the same error rather than masquerading as EOF.
	readErr error
}

func (f *s3File) Read(p []byte) (int, error) {
	if f.readErr != nil {
		return 0, f.readErr
	}
	n, err := f.body.Read(p)
	if err != nil && err != io.EOF {
		f.readErr = err
	}
	return n, err
}

func (f *s3File) Close() error {
	if f.body != nil {
		return f.body.Close()
	}
	return nil
}

func (f *s3File) Stat() (fs.FileInfo, error) {
	return &s3FileInfo{name: baseName(f.name), size: f.size, modTime: f.modTime}, nil
}

type s3Dir struct{ f *FS }

func (d *s3Dir) Read(_ []byte) (int, error) { return 0, fmt.Errorf("read on directory") }
func (d *s3Dir) Close() error               { return nil }
func (d *s3Dir) Stat() (fs.FileInfo, error) {
	return &s3FileInfo{name: ".", isDir: true}, nil
}
func (d *s3Dir) ReadDir(_ int) ([]fs.DirEntry, error) { return d.f.ReadDir(".") }

type s3DirEntry struct {
	name    string
	size    int64
	modTime time.Time
}

func (e *s3DirEntry) Name() string { return e.name }
func (e *s3DirEntry) IsDir() bool  { return false }
func (e *s3DirEntry) Type() fs.FileMode {
	return 0
}
func (e *s3DirEntry) Info() (fs.FileInfo, error) {
	return &s3FileInfo{name: e.name, size: e.size, modTime: e.modTime}, nil
}

type s3FileInfo struct {
	name    string
	size    int64
	modTime time.Time
	isDir   bool
}

func (i *s3FileInfo) Name() string { return i.name }
func (i *s3FileInfo) Size() int64  { return i.size }
func (i *s3FileInfo) Mode() fs.FileMode {
	if i.isDir {
		return fs.ModeDir | 0o755
	}
	return 0o644
}
func (i *s3FileInfo) ModTime() time.Time { return i.modTime }
func (i *s3FileInfo) IsDir() bool        { return i.isDir }
func (i *s3FileInfo) Sys() any           { return nil }

func baseName(p string) string {
	if i := strings.LastIndex(p, "/"); i >= 0 {
		return p[i+1:]
	}
	return p
}
