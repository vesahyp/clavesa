package runlock

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
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// fakeClock is an injectable, advanceable clock.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 6, 13, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// fakeS3 implements S3API with in-memory conditional PUT/GET semantics
// (If-None-Match: * and If-Match: <etag>, both answering 412 on a miss —
// the contract aws-sdk-go-v2/service/s3 v1.96.1 exposes).
type fakeS3 struct {
	mu      sync.Mutex
	objects map[string]fakeObj
	etagSeq int
}

type fakeObj struct {
	body []byte
	etag string
}

func newFakeS3() *fakeS3 { return &fakeS3{objects: map[string]fakeObj{}} }

func (f *fakeS3) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	obj, ok := f.objects[aws.ToString(in.Bucket)+"/"+aws.ToString(in.Key)]
	if !ok {
		return nil, &smithy.GenericAPIError{Code: "NoSuchKey", Message: "key absent"}
	}
	return &s3.GetObjectOutput{
		Body: io.NopCloser(strings.NewReader(string(obj.body))),
		ETag: aws.String(obj.etag),
	}, nil
}

func (f *fakeS3) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := aws.ToString(in.Bucket) + "/" + aws.ToString(in.Key)
	cur, exists := f.objects[key]
	if in.IfNoneMatch != nil && exists {
		return nil, &smithy.GenericAPIError{Code: "PreconditionFailed", Message: "object exists"}
	}
	if in.IfMatch != nil && (!exists || cur.etag != aws.ToString(in.IfMatch)) {
		return nil, &smithy.GenericAPIError{Code: "PreconditionFailed", Message: "etag mismatch"}
	}
	body, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	f.etagSeq++
	etag := fmt.Sprintf("\"etag-%d\"", f.etagSeq)
	f.objects[key] = fakeObj{body: body, etag: etag}
	return &s3.PutObjectOutput{ETag: aws.String(etag)}, nil
}

// doc reads the stored lease for assertions.
func (f *fakeS3) doc(t *testing.T, bucket, key string) leaseDoc {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	obj, ok := f.objects[bucket+"/"+key]
	if !ok {
		t.Fatalf("lease object %s/%s not stored", bucket, key)
	}
	var d leaseDoc
	if err := json.Unmarshal(obj.body, &d); err != nil {
		t.Fatalf("parse stored lease: %v", err)
	}
	return d
}

// lockers returns a same-pipeline Locker factory per backend so every
// protocol test runs against both.
func lockers(t *testing.T, clock *fakeClock) map[string]func() Locker {
	t.Helper()
	dir := t.TempDir()
	fake := newFakeS3()
	return map[string]func() Locker{
		"file": func() Locker {
			l, err := New(dir, "demo", WithClock(clock.Now))
			if err != nil {
				t.Fatalf("New(file): %v", err)
			}
			return l
		},
		"s3": func() Locker {
			l, err := New("s3://bkt", "demo", WithClock(clock.Now), WithS3Client(fake))
			if err != nil {
				t.Fatalf("New(s3): %v", err)
			}
			return l
		},
	}
}

func holder(runID string) Holder {
	return Holder{RunID: runID, Compute: "local", Host: "test-host", PID: os.Getpid()}
}

// TestAcquireFree — both backends grant the lease when no object exists.
func TestAcquireFree(t *testing.T) {
	clock := newFakeClock()
	for name, mk := range lockers(t, clock) {
		t.Run(name, func(t *testing.T) {
			lease, err := mk().Acquire(context.Background(), holder("r1"))
			if err != nil {
				t.Fatalf("Acquire on free lock: %v", err)
			}
			if got := lease.Holder().RunID; got != "r1" {
				t.Errorf("lease holder = %q, want r1", got)
			}
			if err := lease.Release(context.Background()); err != nil {
				t.Errorf("Release: %v", err)
			}
		})
	}
}

// TestAcquireHeld — a live lease rejects the second acquirer with
// *HeldError carrying the current holder.
func TestAcquireHeld(t *testing.T) {
	clock := newFakeClock()
	for name, mk := range lockers(t, clock) {
		t.Run(name, func(t *testing.T) {
			lk := mk()
			lease, err := lk.Acquire(context.Background(), holder("r1"))
			if err != nil {
				t.Fatalf("first Acquire: %v", err)
			}
			defer lease.Release(context.Background())

			_, err = lk.Acquire(context.Background(), holder("r2"))
			var held *HeldError
			if !errors.As(err, &held) {
				t.Fatalf("second Acquire: err = %v, want *HeldError", err)
			}
			if held.Holder.RunID != "r1" {
				t.Errorf("HeldError holder = %q, want r1", held.Holder.RunID)
			}
			for _, want := range []string{"run lock held by run r1", "compute=local", "host=test-host"} {
				if !strings.Contains(held.Error(), want) {
					t.Errorf("HeldError message %q missing %q", held.Error(), want)
				}
			}
		})
	}
}

// TestAcquireReleasedTombstone — a released lease (state tombstone, not
// deletion) is immediately acquirable.
func TestAcquireReleasedTombstone(t *testing.T) {
	clock := newFakeClock()
	for name, mk := range lockers(t, clock) {
		t.Run(name, func(t *testing.T) {
			lk := mk()
			lease, err := lk.Acquire(context.Background(), holder("r1"))
			if err != nil {
				t.Fatalf("first Acquire: %v", err)
			}
			if err := lease.Release(context.Background()); err != nil {
				t.Fatalf("Release: %v", err)
			}
			next, err := lk.Acquire(context.Background(), holder("r2"))
			if err != nil {
				t.Fatalf("Acquire after tombstone: %v", err)
			}
			if got := next.Holder().RunID; got != "r2" {
				t.Errorf("takeover holder = %q, want r2", got)
			}
			next.Release(context.Background())
		})
	}
}

// TestAcquireExpired — within TTL+grace the lease stays held; past
// TTL+grace a competing acquirer takes over atomically.
func TestAcquireExpired(t *testing.T) {
	clock := newFakeClock()
	for name, mk := range lockers(t, clock) {
		t.Run(name, func(t *testing.T) {
			lk := mk()
			if _, err := lk.Acquire(context.Background(), holder("r1")); err != nil {
				t.Fatalf("first Acquire: %v", err)
			}
			// Expired but inside the grace window: still held.
			clock.Advance(TTL + TakeoverGrace - time.Second)
			var held *HeldError
			if _, err := lk.Acquire(context.Background(), holder("r2")); !errors.As(err, &held) {
				t.Fatalf("Acquire inside grace: err = %v, want *HeldError", err)
			}
			// Past expiry + grace: takeover.
			clock.Advance(2 * time.Second)
			lease, err := lk.Acquire(context.Background(), holder("r3"))
			if err != nil {
				t.Fatalf("Acquire past grace: %v", err)
			}
			if got := lease.Holder().RunID; got != "r3" {
				t.Errorf("takeover holder = %q, want r3", got)
			}
			lease.Release(context.Background())
		})
	}
}

// TestHeartbeatExtends — renew pushes expires_at forward by TTL from now.
func TestHeartbeatExtends(t *testing.T) {
	clock := newFakeClock()
	for name, mk := range lockers(t, clock) {
		t.Run(name, func(t *testing.T) {
			lk := mk()
			lease, err := lk.Acquire(context.Background(), holder("r1"))
			if err != nil {
				t.Fatalf("Acquire: %v", err)
			}
			before := lease.doc.ExpiresAt
			clock.Advance(HeartbeatInterval)
			lease.renew(context.Background())
			if lease.Lost() {
				t.Fatal("renew on a healthy lease reported lost")
			}
			if !lease.doc.ExpiresAt.After(before) {
				t.Errorf("expires_at not extended: before=%v after=%v", before, lease.doc.ExpiresAt)
			}
			want := clock.Now().Add(TTL)
			if !lease.doc.ExpiresAt.Equal(want) {
				t.Errorf("expires_at = %v, want %v", lease.doc.ExpiresAt, want)
			}
			lease.Release(context.Background())
		})
	}
}

// TestHeartbeatAfterLoss — when the lease is taken over (TTL lapse), the
// holder's next renew sets the lost flag, logs, and does not panic; a
// later Release is a no-op that doesn't clobber the new holder.
func TestHeartbeatAfterLoss(t *testing.T) {
	clock := newFakeClock()
	for name, mk := range lockers(t, clock) {
		t.Run(name, func(t *testing.T) {
			lk := mk()
			lease, err := lk.Acquire(context.Background(), holder("r1"))
			if err != nil {
				t.Fatalf("Acquire: %v", err)
			}
			// TTL lapses; a second runner takes over.
			clock.Advance(TTL + TakeoverGrace + time.Second)
			usurper, err := lk.Acquire(context.Background(), holder("r2"))
			if err != nil {
				t.Fatalf("takeover Acquire: %v", err)
			}
			lease.renew(context.Background())
			if !lease.Lost() {
				t.Error("renew after takeover: Lost() = false, want true")
			}
			if err := lease.Release(context.Background()); err != nil {
				t.Errorf("Release after loss: %v", err)
			}
			// The usurper's lease must still be intact (not tombstoned by
			// the loser's Release).
			var held *HeldError
			if _, err := lk.Acquire(context.Background(), holder("r3")); !errors.As(err, &held) {
				t.Fatalf("usurper's lease gone after loser Release: err = %v", err)
			}
			if held.Holder.RunID != "r2" {
				t.Errorf("current holder = %q, want r2", held.Holder.RunID)
			}
			usurper.Release(context.Background())
		})
	}
}

// TestReleaseWritesTombstone — Release writes state=released rather than
// deleting, on both backends.
func TestReleaseWritesTombstone(t *testing.T) {
	clock := newFakeClock()

	t.Run("file", func(t *testing.T) {
		dir := t.TempDir()
		lk, err := New(dir, "demo", WithClock(clock.Now))
		if err != nil {
			t.Fatal(err)
		}
		lease, err := lk.Acquire(context.Background(), holder("r1"))
		if err != nil {
			t.Fatalf("Acquire: %v", err)
		}
		if err := lease.Release(context.Background()); err != nil {
			t.Fatalf("Release: %v", err)
		}
		data, err := os.ReadFile(filepath.Join(dir, "_locks", "demo.run.json"))
		if err != nil {
			t.Fatalf("lease file gone after Release (tombstone expected): %v", err)
		}
		var doc leaseDoc
		if err := json.Unmarshal(data, &doc); err != nil {
			t.Fatal(err)
		}
		if doc.State != stateReleased {
			t.Errorf("state = %q, want released", doc.State)
		}
	})

	t.Run("s3", func(t *testing.T) {
		fake := newFakeS3()
		lk, err := New("s3://bkt", "demo", WithClock(clock.Now), WithS3Client(fake))
		if err != nil {
			t.Fatal(err)
		}
		lease, err := lk.Acquire(context.Background(), holder("r1"))
		if err != nil {
			t.Fatalf("Acquire: %v", err)
		}
		if err := lease.Release(context.Background()); err != nil {
			t.Fatalf("Release: %v", err)
		}
		doc := fake.doc(t, "bkt", "demo/_locks/run.json")
		if doc.State != stateReleased {
			t.Errorf("state = %q, want released", doc.State)
		}
	})
}

// TestFileContention — N goroutines race a free file lock; exactly one
// wins (O_CREATE|O_EXCL is the atomic arbiter).
func TestFileContention(t *testing.T) {
	dir := t.TempDir()
	const n = 16
	var wins int32
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			lk, err := New(dir, "demo")
			if err != nil {
				t.Errorf("New: %v", err)
				return
			}
			lease, err := lk.Acquire(context.Background(), holder(fmt.Sprintf("r%d", i)))
			if err == nil {
				mu.Lock()
				wins++
				mu.Unlock()
				_ = lease // hold; never release inside the race window
				return
			}
			var held *HeldError
			if !errors.As(err, &held) {
				t.Errorf("loser got non-HeldError: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if wins != 1 {
		t.Errorf("contended acquire: %d winners, want exactly 1", wins)
	}
}

// TestS3CreateRaceReportsWinner — losing the If-None-Match create race
// surfaces the winner's holder, not a raw 412.
func TestS3CreateRaceReportsWinner(t *testing.T) {
	fake := newFakeS3()
	lk1, _ := New("s3://bkt", "demo", WithS3Client(fake))
	lk2, _ := New("s3://bkt", "demo", WithS3Client(fake))
	if _, err := lk1.Acquire(context.Background(), holder("winner")); err != nil {
		t.Fatalf("first Acquire: %v", err)
	}
	_, err := lk2.Acquire(context.Background(), holder("loser"))
	var held *HeldError
	if !errors.As(err, &held) {
		t.Fatalf("err = %v, want *HeldError", err)
	}
	if held.Holder.RunID != "winner" {
		t.Errorf("reported holder = %q, want winner", held.Holder.RunID)
	}
}

// TestStartHeartbeatRenewsAndStops — the heartbeat goroutine renews on its
// interval and Release stops it cleanly (no goroutine leak / double close).
func TestStartHeartbeatRenewsAndStops(t *testing.T) {
	dir := t.TempDir()
	lk, err := New(dir, "demo", WithHeartbeatInterval(10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	lease, err := lk.Acquire(context.Background(), holder("r1"))
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	before := lease.doc.ExpiresAt
	lease.StartHeartbeat()
	deadline := time.Now().Add(2 * time.Second)
	for {
		lease.mu.Lock()
		extended := lease.doc.ExpiresAt.After(before)
		lease.mu.Unlock()
		if extended {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("heartbeat goroutine never renewed the lease")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("Release: %v", err)
	}
	// Idempotent.
	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("second Release: %v", err)
	}
}
