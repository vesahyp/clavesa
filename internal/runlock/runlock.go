// Package runlock serializes pipeline runs with a lease lock stored in the
// warehouse (ADR-024). Every compute — Lambda, a laptop, a second laptop —
// acquires the same lock before running, which is what protects the
// single-driver Delta log (S3SingleDriverLogStore tolerates exactly one
// Spark driver writing a table at a time).
//
// One protocol, two backends behind the Locker interface:
//
//   - S3 (cloud warehouse): object `s3://<bucket>/<pipeline>/_locks/run.json`,
//     CAS via conditional PUT (If-None-Match: * to create, If-Match: <etag>
//     to take over / renew / release).
//   - File (local warehouse): `<warehouse>/_locks/<pipeline>.run.json`,
//     create via O_CREATE|O_EXCL, take over / renew / release via nonce
//     check + write-temp + rename.
//
// The lease document is JSON with a TTL: holders renew (heartbeat) every
// HeartbeatInterval; a competing acquirer may take over once the lease is
// expired past TakeoverGrace (clock-skew allowance) or carries an explicit
// `state: "released"` tombstone. Release writes the tombstone rather than
// deleting — conditional DELETE has no If-Match semantics on S3, so the
// tombstone is the only race-free "free" signal.
package runlock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/vesahyp/clavesa/internal/version"
)

const (
	// TTL is how long a lease stays valid without a heartbeat renewal.
	TTL = 120 * time.Second
	// HeartbeatInterval is how often StartHeartbeat renews the lease.
	HeartbeatInterval = 30 * time.Second
	// TakeoverGrace pads TTL expiry before a competing acquirer may take
	// over — allowance for clock skew between the holder and the acquirer.
	TakeoverGrace = 30 * time.Second

	stateHeld     = "held"
	stateReleased = "released"
)

// Holder identifies who holds (or wants) the run lock.
type Holder struct {
	RunID   string `json:"run_id"`
	Compute string `json:"compute"` // "local" | "lambda"
	Host    string `json:"host"`
	PID     int    `json:"pid"`
}

// leaseDoc is the JSON lease document both backends store.
type leaseDoc struct {
	Holder        Holder    `json:"holder"`
	AcquiredAt    time.Time `json:"acquired_at"`
	ExpiresAt     time.Time `json:"expires_at"`
	TTLSeconds    int       `json:"ttl_s"`
	Nonce         string    `json:"nonce"`
	State         string    `json:"state"` // held | released
	ModuleVersion string    `json:"module_version"`
}

// HeldError reports that the run lock is currently held by another run.
// Callers map it to their "already in progress" surface (the service layer
// wraps it under errs.ErrRunInFlight so the HTTP 409 path keeps working).
type HeldError struct {
	Holder     Holder
	AcquiredAt time.Time
	ExpiresAt  time.Time
}

func (e *HeldError) Error() string {
	age := time.Since(e.AcquiredAt).Round(time.Second)
	if age < 0 {
		age = 0
	}
	return fmt.Sprintf("run lock held by run %s (compute=%s, host=%s, acquired %s ago)",
		e.Holder.RunID, e.Holder.Compute, e.Holder.Host, age)
}

// Backend-internal sentinels. errNotExist: no lease object stored.
// errRaced: the conditional write lost (object appeared, etag/nonce moved).
var (
	errNotExist = errors.New("runlock: lease does not exist")
	errRaced    = errors.New("runlock: conditional write raced")
)

// backend abstracts the storage CAS primitives the protocol needs. Both
// implementations expose get / create-if-absent / replace-if-unchanged
// keyed by an opaque CAS token (S3: ETag; file: lease nonce).
type backend interface {
	// get returns the stored lease and its CAS token, or errNotExist.
	get(ctx context.Context) (*leaseDoc, string, error)
	// create stores doc iff no lease object exists. Returns errRaced on
	// conflict, else the new CAS token.
	create(ctx context.Context, doc *leaseDoc) (string, error)
	// replace stores doc iff the stored object still matches token.
	// Returns errRaced on conflict, else the new CAS token.
	replace(ctx context.Context, token string, doc *leaseDoc) (string, error)
	// where returns a human-readable lock location for log lines.
	where() string
}

// Locker acquires the per-pipeline run lease.
type Locker interface {
	Acquire(ctx context.Context, h Holder) (*Lease, error)
}

// Option configures New.
type Option func(*config)

type config struct {
	now        func() time.Time
	s3         S3API
	hbInterval time.Duration
}

// WithClock injects a clock for tests. Real callers default to time.Now.
func WithClock(now func() time.Time) Option {
	return func(c *config) { c.now = now }
}

// WithS3Client supplies the S3 client an s3:// warehouse backend requires.
func WithS3Client(client S3API) Option {
	return func(c *config) { c.s3 = client }
}

// WithHeartbeatInterval overrides the renewal cadence (tests only).
func WithHeartbeatInterval(d time.Duration) Option {
	return func(c *config) { c.hbInterval = d }
}

// New returns the Locker for one pipeline's run lease, picking the backend
// by warehouse URI prefix: `s3://…` → the conditional-PUT S3 backend
// (requires WithS3Client), anything else is treated as a local warehouse
// directory → the file backend.
func New(warehouseURI, pipeline string, opts ...Option) (Locker, error) {
	cfg := &config{now: time.Now, hbInterval: HeartbeatInterval}
	for _, o := range opts {
		o(cfg)
	}
	var be backend
	if strings.HasPrefix(warehouseURI, "s3://") {
		if cfg.s3 == nil {
			return nil, errors.New("runlock: s3 warehouse requires an S3 client (WithS3Client)")
		}
		bucket, prefix := splitS3URI(warehouseURI)
		if bucket == "" {
			return nil, fmt.Errorf("runlock: malformed s3 warehouse URI %q", warehouseURI)
		}
		// `s3://<bucket>/<pipeline>/_locks/run.json` — deliberately under
		// the pipeline prefix so the existing per-pipeline IAM grant
		// (`${bucket}/${pipeline}/*/*`) covers it without a policy change.
		be = &s3Backend{
			client: cfg.s3,
			bucket: bucket,
			key:    path.Join(prefix, pipeline, "_locks", "run.json"),
		}
	} else {
		be = &fileBackend{
			path: filepath.Join(warehouseURI, "_locks", pipeline+".run.json"),
		}
	}
	return &lock{be: be, now: cfg.now, hbInterval: cfg.hbInterval}, nil
}

// splitS3URI splits "s3://bucket[/prefix...]" into (bucket, prefix).
func splitS3URI(uri string) (bucket, prefix string) {
	rest := strings.TrimPrefix(uri, "s3://")
	bucket, prefix, _ = strings.Cut(rest, "/")
	return bucket, strings.Trim(prefix, "/")
}

type lock struct {
	be         backend
	now        func() time.Time
	hbInterval time.Duration
}

// Acquire takes the lease or returns *HeldError carrying the current
// holder. Free (no object), tombstoned (state=released), and expired past
// TakeoverGrace all acquire; anything else is held. The successful caller
// owns the returned Lease and must Release it; StartHeartbeat keeps it
// alive across a long run.
func (l *lock) Acquire(ctx context.Context, h Holder) (*Lease, error) {
	doc := l.newDoc(h)
	cur, token, err := l.be.get(ctx)
	switch {
	case errors.Is(err, errNotExist):
		token, err = l.be.create(ctx, doc)
	case err != nil:
		return nil, fmt.Errorf("runlock: read lease at %s: %w", l.be.where(), err)
	case cur.State == stateReleased || l.now().After(cur.ExpiresAt.Add(TakeoverGrace)):
		// Tombstoned or expired past the grace window: atomic takeover —
		// the conditional replace fails if anyone else moved it first.
		token, err = l.be.replace(ctx, token, doc)
	default:
		return nil, &HeldError{Holder: cur.Holder, AcquiredAt: cur.AcquiredAt, ExpiresAt: cur.ExpiresAt}
	}
	if errors.Is(err, errRaced) {
		// Lost the create/takeover race. Report whoever won.
		if won, _, gerr := l.be.get(ctx); gerr == nil {
			return nil, &HeldError{Holder: won.Holder, AcquiredAt: won.AcquiredAt, ExpiresAt: won.ExpiresAt}
		}
		return nil, &HeldError{Holder: Holder{RunID: "unknown"}, AcquiredAt: l.now()}
	}
	if err != nil {
		return nil, fmt.Errorf("runlock: acquire lease at %s: %w", l.be.where(), err)
	}
	return &Lease{
		be:         l.be,
		now:        l.now,
		hbInterval: l.hbInterval,
		doc:        *doc,
		token:      token,
	}, nil
}

func (l *lock) newDoc(h Holder) *leaseDoc {
	now := l.now()
	return &leaseDoc{
		Holder:        h,
		AcquiredAt:    now,
		ExpiresAt:     now.Add(TTL),
		TTLSeconds:    int(TTL / time.Second),
		Nonce:         newNonce(),
		State:         stateHeld,
		ModuleVersion: version.Module,
	}
}

// Lease is a held run lock. The owner calls StartHeartbeat to keep it
// renewed and Release when the run is observably terminal. Lost reports
// whether a renewal discovered the lock was taken away (TTL lapse + a
// competing takeover) — by design the in-flight run continues anyway:
// aborting a mid-flight Spark write is worse than racing, and the Delta
// commit is the last defense.
type Lease struct {
	be         backend
	now        func() time.Time
	hbInterval time.Duration

	mu       sync.Mutex
	doc      leaseDoc
	token    string
	lost     bool
	released bool

	hbStarted bool
	stopOnce  sync.Once
	stop      chan struct{}
	done      chan struct{}
}

// Holder returns the identity this lease was acquired with.
func (l *Lease) Holder() Holder {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.doc.Holder
}

// Lost reports whether a renewal or release discovered the lease had been
// taken over. Callers inspect it after the run; they must NOT abort the
// in-flight run on it.
func (l *Lease) Lost() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lost
}

// StartHeartbeat spawns the renewal goroutine (every hbInterval, default
// HeartbeatInterval). Safe to call once; Release stops it. No-op when the
// lease is already released.
func (l *Lease) StartHeartbeat() {
	l.mu.Lock()
	if l.hbStarted || l.released {
		l.mu.Unlock()
		return
	}
	l.hbStarted = true
	l.stop = make(chan struct{})
	l.done = make(chan struct{})
	interval := l.hbInterval
	l.mu.Unlock()

	go func() {
		defer close(l.done)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-l.stop:
				return
			case <-t.C:
				l.renew(context.Background())
				if l.Lost() {
					return
				}
			}
		}
	}()
}

// renew extends expires_at by TTL via a conditional replace. A raced
// replace means the lock was lost: log loudly, set the lost flag, and
// return — never abort the in-flight run from here. Transient backend
// errors are logged and retried on the next tick.
func (l *Lease) renew(ctx context.Context) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released || l.lost {
		return
	}
	doc := l.doc
	doc.ExpiresAt = l.now().Add(TTL)
	token, err := l.be.replace(ctx, l.token, &doc)
	switch {
	case errors.Is(err, errRaced):
		l.lost = true
		fmt.Fprintf(os.Stderr,
			"[clavesa] RUN LOCK LOST at %s: lease for run %s was taken over mid-run — continuing the in-flight run anyway (the Delta commit is the last defense against the race)\n",
			l.be.where(), l.doc.Holder.RunID)
	case err != nil:
		fmt.Fprintf(os.Stderr, "[clavesa] run lock heartbeat at %s failed (will retry): %v\n",
			l.be.where(), err)
	default:
		l.doc = doc
		l.token = token
	}
}

// Release stops the heartbeat and writes the `state: "released"` tombstone
// so the next acquirer takes over immediately instead of waiting out the
// TTL. Idempotent. A raced tombstone write means someone already took the
// lock over (after a TTL lapse) — nothing left to free, not an error.
func (l *Lease) Release(ctx context.Context) error {
	l.stopHeartbeat()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return nil
	}
	l.released = true
	if l.lost {
		return nil
	}
	doc := l.doc
	doc.State = stateReleased
	doc.ExpiresAt = l.now()
	if _, err := l.be.replace(ctx, l.token, &doc); err != nil {
		if errors.Is(err, errRaced) {
			l.lost = true
			return nil
		}
		return fmt.Errorf("runlock: release lease at %s: %w", l.be.where(), err)
	}
	return nil
}

func (l *Lease) stopHeartbeat() {
	l.mu.Lock()
	started := l.hbStarted
	l.mu.Unlock()
	if !started {
		return
	}
	l.stopOnce.Do(func() { close(l.stop) })
	<-l.done
}

func newNonce() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Mirrors service.newRunID's stance: crypto/rand failing is
		// exceptional; degrade to time bits rather than panic mid-run.
		t := time.Now().UnixNano()
		for i := 0; i < 16; i++ {
			b[i] = byte(t >> (8 * (i % 8)))
		}
	}
	return hex.EncodeToString(b[:])
}
