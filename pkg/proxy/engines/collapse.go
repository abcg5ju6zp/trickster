/*
 * Copyright 2018 The Trickster Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package engines

import (
	"context"
	stderrors "errors"
	"io"
	"mime"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/trickstercache/trickster/v2/pkg/observability/keys"
	"github.com/trickstercache/trickster/v2/pkg/observability/logging"
	"github.com/trickstercache/trickster/v2/pkg/observability/logging/logger"
	"github.com/trickstercache/trickster/v2/pkg/observability/logging/redact"
	perrs "github.com/trickstercache/trickster/v2/pkg/proxy/errors"
	"github.com/trickstercache/trickster/v2/pkg/proxy/headers"
	"github.com/trickstercache/trickster/v2/pkg/proxy/methods"
	po "github.com/trickstercache/trickster/v2/pkg/proxy/paths/options"
	"github.com/trickstercache/trickster/v2/pkg/proxy/request"

	"golang.org/x/net/http/httpguts"
)

// collapseEligible reports whether a response may be fanned out to multiple
// clients. Collapsing delivers identical bytes to everyone who joins, which is
// strictly stronger than caching, so anything RFC 9111 forbids a shared cache
// from storing must also refuse to collapse. Default is to refuse: a missed
// collapse costs a fetch; a wrong one is cross-user disclosure.
func collapseEligible(r *http.Request, statusCode int, h http.Header, pc *po.Options) bool {
	if statusCode != http.StatusOK {
		return false
	}
	if h.Get(headers.NameSetCookie) != "" {
		return false
	}
	if base, _, err := mime.ParseMediaType(h.Get(headers.NameContentType)); err == nil &&
		base == ContentTypeEventStream {
		return false
	}
	var isPublic bool
	for _, cc := range h.Values(headers.NameCacheControl) {
		for tok := range strings.SplitSeq(cc, ",") {
			tok = strings.ToLower(strings.TrimSpace(tok))
			d, _, _ := strings.Cut(tok, "=")
			switch d {
			case headers.ValuePrivate, headers.ValueNoStore:
				return false
			case headers.ValuePublic, headers.ValueSharedMaxAge, headers.ValueMustRevalidate:
				isPublic = true
			}
		}
	}
	// RFC 9111 3.5: an authorized response requires explicit shared-cache
	// permission before it may be reused for anyone else
	if r.Header.Get(headers.NameAuthorization) != "" && !isPublic {
		return false
	}
	// a Vary field not reflected in the collapse key means two joiners could
	// legitimately deserve different bytes
	for _, vv := range h.Values(headers.NameVary) {
		for f := range strings.SplitSeq(vv, ",") {
			f = strings.TrimSpace(f)
			if f == "" {
				continue
			}
			if f == "*" || pc == nil || !slices.ContainsFunc(pc.CacheKeyHeaders,
				func(k string) bool { return strings.EqualFold(k, f) }) {
				return false
			}
		}
	}
	return true
}

func requestIsUpgrade(r *http.Request) bool {
	return r.Header.Get(headers.NameUpgrade) != "" &&
		httpguts.HeaderValuesContainsToken(r.Header[headers.NameConnection], "Upgrade")
}

// collapseRoundSeq gives each registry entry a process-wide stable identity so
// logs, tests and CompareAndDelete guards can tell one round for a key apart
// from the next one.
var collapseRoundSeq atomic.Uint64

// collapseRegistry tracks in-flight progressive collapse rounds by cache key.
// The zero value is not usable; construct with newCollapseRegistry.
type collapseRegistry struct {
	rounds sync.Map // string -> *collapseRound

	shutting  atomic.Bool
	closeOnce sync.Once
	closeCh   chan struct{}
}

func newCollapseRegistry() *collapseRegistry {
	return &collapseRegistry{closeCh: make(chan struct{})}
}

func (r *collapseRegistry) shuttingDown() bool { return r.shutting.Load() }

func (r *collapseRegistry) done() <-chan struct{} { return r.closeCh }

// shutdown reclaims every in-flight round exactly once: owner upstream reads
// are cancelled, their response bodies closed, shared byte streams failed with
// a distinguishable shutdown outcome and the registrations removed. Requests
// arriving afterwards take the normal single-request path.
func (r *collapseRegistry) shutdown() {
	r.shutting.Store(true)
	r.closeOnce.Do(func() { close(r.closeCh) })
	r.rounds.Range(func(_, value any) bool {
		value.(*collapseRound).abandon(perrs.ErrCollapseClosing)
		return true
	})
}

// collapses tracks in-flight progressive collapse rounds by cache key.
var collapses = newCollapseRegistry()

// CloseInFlightCollapses interrupts all in-memory collapse rounds during
// service shutdown. Detached owner fetches are reclaimed so the listener drain
// does not wait on an upstream nobody is listening to, and every blocked
// client receives a distinguishable terminal error rather than a clean EOF.
func CloseInFlightCollapses() {
	collapses.shutdown()
	shutdownPCFSessions()
}

// collapseRound is the stable execution identity of one collapse for one cache
// key: exactly one owner fetches the upstream, every joiner binds to the same
// response headers and the same byte sequence for the round's lifetime. Only
// the owner goroutine publishes outcomes or removes the registration, and it
// may only ever remove its own identity, so a late goroutine can never close or
// republish a successor round for the same key.
type collapseRound struct {
	id  uint64
	key string
	reg *collapseRegistry

	// cond guards resolved/bypass/pcf plus the ownerDone/abandoned lifecycle.
	cond     *sync.Cond
	resolved bool
	// bypass releases waiters to the normal single-request path because no
	// shared response exists (ineligible response or failure before headers).
	bypass bool
	pcf    ProgressiveCollapseForwarder

	// parties counts handler goroutines bound to this round: the leader plus
	// every follower from the moment it loads the entry until its own response
	// is fully concluded. It includes clients blocked awaiting headers.
	parties atomic.Int32

	// cancel reclaims the owner's detached upstream read; tracker closes the
	// concrete response body. Both are set by the leader before fetch start.
	cancel  context.CancelCauseFunc
	tracker *collapseBodyTracker

	ownerDone bool // the owner goroutine has published its terminal outcome
	abandoned bool // the round was reclaimed before the owner finished
}

func newCollapseRound(key string) *collapseRound {
	return &collapseRound{
		id:   collapseRoundSeq.Add(1),
		key:  key,
		reg:  collapses,
		cond: sync.NewCond(&sync.Mutex{}),
	}
}

// addParty binds one more handler to the round. It must be paired with leave.
func (rd *collapseRound) addParty() { rd.parties.Add(1) }

// leave removes this handler's binding alone. When the last binding goes away
// while the owner may still produce shared bytes, the round is abandoned: the
// upstream read is reclaimed, waiters released, the entry removed, so later
// requests safely claim a fresh round.
func (rd *collapseRound) leave() {
	if rd.parties.Add(-1) > 0 {
		return
	}
	rd.abandon(perrs.ErrCollapseAbandoned)
}

func (rd *collapseRound) wake() {
	rd.cond.L.Lock()
	rd.cond.Broadcast()
	rd.cond.L.Unlock()
}

// abandon reclaims an in-flight round. It is idempotent and inert once the
// owner has published its terminal outcome. The registry removal is identity
// guarded, so an owner from a dead round can never delete a successor round.
func (rd *collapseRound) abandon(cause error) {
	rd.cond.L.Lock()
	if rd.ownerDone || rd.abandoned {
		rd.cond.L.Unlock()
		return
	}
	rd.abandoned = true
	needResolve := !rd.resolved
	pcf := rd.pcf
	cancel := rd.cancel
	tracker := rd.tracker
	if needResolve {
		rd.resolved = true
		rd.bypass = true
	}
	rd.cond.Broadcast()
	rd.cond.L.Unlock()

	// reclaim the owner's upstream read and body outside the lock
	if cancel != nil {
		cancel(cause)
	}
	if tracker != nil {
		tracker.closeBody()
	}
	// fail the shared stream so any blocked joiner learns the true outcome
	// instead of treating drained bytes as a complete object
	if pcf != nil {
		pcf.CloseWithError(cause)
	}
	rd.reg.rounds.CompareAndDelete(rd.key, rd)
}

// resolveJoin publishes the shared stream once headers prove the response is
// collapsible.
func (rd *collapseRound) resolveJoin(pcf ProgressiveCollapseForwarder) {
	rd.cond.L.Lock()
	if !rd.resolved {
		rd.resolved = true
		rd.pcf = pcf
	}
	rd.cond.Broadcast()
	rd.cond.L.Unlock()
}

// resolveBypass releases every waiter to the normal single-request path.
func (rd *collapseRound) resolveBypass() {
	rd.cond.L.Lock()
	if !rd.resolved {
		rd.resolved = true
		rd.bypass = true
	}
	rd.cond.Broadcast()
	rd.cond.L.Unlock()
}

// await blocks until eligibility is decided. It returns the shared PCF to join,
// bypass==true when the caller must conclude independently, or ctx's error
// when THIS waiter left (client cancellation) before headers arrived.
func (rd *collapseRound) await(ctx context.Context) (ProgressiveCollapseForwarder, bool, error) {
	stop := context.AfterFunc(ctx, rd.wake)
	defer stop()
	rd.cond.L.Lock()
	defer rd.cond.L.Unlock()
	for !rd.resolved {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		if rd.reg.shuttingDown() {
			return nil, false, perrs.ErrCollapseClosing
		}
		rd.cond.Wait()
	}
	return rd.pcf, rd.bypass, nil
}

// CollapsedPassthrough wraps a passthrough handler with Progressive Collapsed
// Forwarding: concurrent requests for the same key share one upstream fetch.
// Eligibility is only knowable once the upstream's headers arrive, so the
// leader fetches optimistically and followers wait on the pending round.
func CollapsedPassthrough(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rsc := request.GetResources(r)
		if rsc == nil || rsc.BackendOptions == nil ||
			!methods.IsCacheable(r.Method) || requestIsUpgrade(r) {
			inner.ServeHTTP(w, r)
			return
		}
		// a closing server starts no new shared rounds; the single-request
		// path keeps working until the listener drain closes the connection
		if collapses.shuttingDown() {
			inner.ServeHTTP(w, r)
			return
		}
		o := rsc.BackendOptions
		pr := newProxyRequest(r, w)
		key := ComposeCacheKey(o.Name, o.CacheKeyPrefix, "", pr.DeriveCacheKey(""))

		for {
			rd := newCollapseRound(key)
			actual, loaded := collapses.rounds.LoadOrStore(key, rd)
			if !loaded {
				rd.lead(inner, w, r, int64(o.MaxObjectSizeBytes))
				return
			}
			// follower: bind before inspecting state, so an abandoning owner
			// cannot reach zero parties and delete the entry out from under us
			rd = actual.(*collapseRound)
			rd.addParty()
			rd.cond.L.Lock()
			dead := rd.abandoned
			rd.cond.L.Unlock()
			if dead {
				// this round was reclaimed in the tiny load/bind window; drop
				// the binding and safely claim a fresh round instead
				rd.leave()
				continue
			}
			collapseAsFollower(rd, inner, w, r)
			return
		}
	})
}

// collapseAsFollower waits on an existing round, then either joins its byte
// stream or concludes independently. The binding is always released exactly
// once, on every path.
func collapseAsFollower(rd *collapseRound, inner http.Handler, w http.ResponseWriter, r *http.Request) {
	defer rd.leave()
	pcf, bypass, err := rd.await(r.Context())
	if err != nil {
		if r.Context().Err() != nil {
			// this client alone went away before headers; the owner and every
			// other joiner keep running
			return
		}
		// shutdown while waiting for headers: nothing was committed yet, so
		// the distinguishable conclusion is a fresh single-request attempt,
		// bounded by the listener drain
		inner.ServeHTTP(w, r)
		return
	}
	if bypass || pcf == nil {
		// non-collapsible response or a pre-header owner failure: no shared
		// response exists, conclude this request normally
		inner.ServeHTTP(w, r)
		return
	}
	joinCollapse(r.Context(), pcf, w, r)
}

func joinCollapse(ctx context.Context, pcf ProgressiveCollapseForwarder,
	w http.ResponseWriter, r *http.Request,
) {
	resp := pcf.GetResp()
	writer := PrepareResponseWriter(w, resp.StatusCode, resp.Header, nil)
	err := pcf.AddClientContext(ctx, streamWriter(writer, resp))
	if err == nil {
		return
	}
	if stderrors.Is(err, context.Canceled) || stderrors.Is(err, context.DeadlineExceeded) {
		return
	}
	logger.Error("collapsed client stream failed",
		logging.Pairs{keys.URL: redact.URL(r.URL), keys.Error: err.Error()})
	abortOnCopyError(w, r, err)
}

// lead owns the round: one detached upstream fetch, with the leader itself
// bound as a regular joiner once headers arrive. The fetch is detached from
// the leader client's cancellation (other clients may need the bytes) but
// owned by the round, which can reclaim it on abandonment or shutdown.
func (rd *collapseRound) lead(inner http.Handler, w http.ResponseWriter,
	r *http.Request, maxSize int64,
) {
	rd.addParty()
	rsc := request.GetResources(r)
	var pc *po.Options
	if rsc != nil {
		pc = rsc.PathConfig
	}
	tracker := newCollapseBodyTracker()
	rd.tracker = tracker

	ctx, cancel := context.WithCancelCause(context.WithoutCancel(r.Context()))
	rd.cancel = cancel
	// bind the owner fetch to the round's lifecycle instead of any one
	// client's: it is reclaimed either when shutdown begins or when every
	// party abandons the round (via cancel)
	go func() {
		select {
		case <-rd.reg.done():
			cancel(perrs.ErrCollapseClosing)
			tracker.closeBody()
		case <-ctx.Done():
		}
	}()

	lr := r.WithContext(withCollapseTracker(ctx, tracker))
	cw := &collapseCapture{
		rd:      rd,
		key:     rd.key,
		req:     r,
		pc:      pc,
		leader:  w,
		maxSize: maxSize,
		header:  make(http.Header),
		tracker: tracker,
	}
	var wg sync.WaitGroup
	wg.Add(1)
	goWithRecover("collapse.owner", func() {
		defer wg.Done()
		defer cancel(nil) // releases the shutdown watcher
		defer cw.finish()
		inner.ServeHTTP(cw, lr)
	})

	pcf, bypass, err := rd.await(r.Context())
	if err == nil && !bypass && pcf != nil {
		// join mode is fixed once headers resolved: the capture feeds only
		// the PCF and never touches w from the owner goroutine, so the leader
		// frame may return the moment its own stream concludes
		joinCollapse(r.Context(), pcf, w, r)
		rd.leave()
		return
	}
	// leader gone before headers, bypass (ineligible response or owner failure
	// before headers), or shutdown: the capture may still be writing this
	// response straight into w, which is only valid while this handler frame
	// is on the stack, so outlive the owner fetch. The fetch stays detached
	// from this client's cancellation; when every party leaves the round is
	// abandoned, which cancels the owner and releases this wait promptly.
	wg.Wait()
	rd.leave()
}

// collapseCapture is the ResponseWriter handed to the detached owner fetch.
// WriteHeader is the moment eligibility becomes decidable: an eligible
// response feeds a PCF that followers join; anything else streams straight
// through to the leader's own client with no buffering.
type collapseCapture struct {
	rd      *collapseRound
	key     string
	req     *http.Request
	pc      *po.Options
	leader  http.ResponseWriter
	maxSize int64
	header  http.Header
	tracker *collapseBodyTracker

	pcf      ProgressiveCollapseForwarder
	tee      io.Writer
	written  int64
	declared int64
	closed   bool
	closeErr error
}

func (c *collapseCapture) Header() http.Header { return c.header }

func (c *collapseCapture) WriteHeader(code int) {
	// 1xx responses are informational and do not resolve eligibility; they
	// cannot be relayed through a collapse, so they are dropped here
	if code < http.StatusOK {
		return
	}
	if c.pcf != nil || c.tee != nil {
		return
	}
	c.declared = -1
	if v := c.header.Get(headers.NameContentLength); v != "" {
		if cl, err := strconv.ParseInt(v, 10, 64); err == nil {
			c.declared = cl
		}
	}
	if collapseEligible(c.req, code, c.header, c.pc) &&
		(c.declared < 0 || c.declared < c.maxSize) {
		resp := &http.Response{
			StatusCode:    code,
			Header:        c.header.Clone(),
			ContentLength: c.declared,
			Request:       c.req,
		}
		if pcf := NewPCF(resp, c.declared, c.maxSize); pcf != nil {
			c.pcf = pcf
			// the tracker is the sole authority over the owner's read outcome
			// in join mode; translate its terminal read error into EOF so
			// ReverseProxy does not panic for a truncation finish() reports
			c.tracker.suppressReadErrors()
			c.rd.resolveJoin(pcf)
			return
		}
	}
	// not collapsible: release followers to fetch independently and deliver
	// this response only to the leader, unbuffered
	c.rd.resolveBypass()
	writer := PrepareResponseWriter(c.leader, code, c.header, nil)
	c.tee = streamWriter(writer, &http.Response{
		StatusCode: code, Header: c.header, ContentLength: c.declared,
	})
}

func (c *collapseCapture) Write(b []byte) (int, error) {
	if c.pcf == nil && c.tee == nil {
		c.WriteHeader(http.StatusOK)
	}
	var n int
	var err error
	if c.pcf != nil {
		n, err = c.pcf.Write(b)
		if err != nil && !c.closed {
			c.closed = true
			c.closeErr = err
			c.pcf.CloseWithError(err)
		}
		if err != nil {
			logger.Error("collapsed upstream write failed",
				logging.Pairs{keys.Key: c.key, keys.Error: err.Error()})
		}
	} else {
		n, err = c.tee.Write(b)
	}
	c.written += int64(n)
	return n, err
}

// Flush satisfies ReverseProxy's streaming path; PCF distribution provides its
// own pacing, and the tee writer flushes per-write already.
func (c *collapseCapture) Flush() {}

// finish publishes the round's terminal outcome exactly once and unregisters
// the round identity. The owner's read side is authoritative: a write-side
// failure, a non-EOF read error, or fewer bytes than declared all fail every
// attached client and keep the partial bytes unreusable; only a whole, clean
// read closes the PCF successfully.
func (c *collapseCapture) finish() {
	rd := c.rd
	rd.cond.L.Lock()
	if rd.abandoned {
		// the round was already reclaimed (all clients gone or shutdown); a
		// late owner never republishes, closes the successor round's PCF, or
		// removes anything but its own dead registration
		rd.ownerDone = true
		rd.cond.Broadcast()
		rd.cond.L.Unlock()
		rd.reg.rounds.CompareAndDelete(rd.key, rd)
		return
	}
	if c.pcf != nil && !c.closed {
		if cause := c.terminalCause(); cause != nil {
			logger.Error("collapsed upstream response incomplete",
				logging.Pairs{
					keys.Key:   c.key,
					keys.Error: cause.Error(),
					"declared": strconv.FormatInt(c.declared, 10),
					"received": strconv.FormatInt(c.written, 10),
				})
			c.pcf.CloseWithError(cause)
		} else {
			c.pcf.Close()
		}
	}
	if !rd.resolved {
		// the owner fetch ended before any header was committed (e.g. origin
		// unreachable): no shared response exists, release everyone to a fresh
		// single-request conclusion
		rd.resolved = true
		rd.bypass = true
		rd.cond.Broadcast()
	}
	rd.ownerDone = true
	rd.cond.L.Unlock()
	rd.reg.rounds.CompareAndDelete(rd.key, rd)
}

// terminalCause derives the owner-side failure, if any. The read-side tracker
// catches stream interruptions that never surface as Write errors, including a
// chunked origin closing mid-body; the declared-length check is a backstop.
func (c *collapseCapture) terminalCause() error {
	if c.closeErr != nil {
		return c.closeErr
	}
	if c.tracker != nil {
		if err := c.tracker.outcome(c.req.Method, c.declared); err != nil {
			return err
		}
	}
	if c.req.Method != http.MethodHead && c.declared > 0 && c.written < c.declared {
		return io.ErrUnexpectedEOF
	}
	return nil
}

// collapseTrackerKey carries the owner's read-side tracker on the detached
// request context into the passthrough's ModifyResponse hook.
type collapseTrackerKeyType struct{}

var collapseTrackerKey collapseTrackerKeyType

func withCollapseTracker(ctx context.Context, t *collapseBodyTracker) context.Context {
	return context.WithValue(ctx, collapseTrackerKey, t)
}

func collapseTrackerFromContext(ctx context.Context) *collapseBodyTracker {
	t, _ := ctx.Value(collapseTrackerKey).(*collapseBodyTracker)
	return t
}

// collapseBodyTracker wraps an owner response body so the collapse learns the
// authoritative read outcome: a clean EOF at the advertised length succeeds,
// any other terminal error or a short declared body fails the round.
type collapseBodyTracker struct {
	mu       sync.Mutex
	declared int64
	n        int64
	readErr  error
	suppress bool
	body     io.Closer
}

func newCollapseBodyTracker() *collapseBodyTracker { return &collapseBodyTracker{} }

// track installs the tracker around rc. The passthrough transport may already
// have wrapped the body (e.g. an idle timeout); the tracker stays outermost.
func (t *collapseBodyTracker) track(rc io.ReadCloser, declared int64) io.ReadCloser {
	t.mu.Lock()
	t.body = rc
	t.declared = declared
	t.mu.Unlock()
	return &trackedCollapseBody{t: t, rc: rc}
}

// suppressReadErrors makes the wrapped reader translate terminal non-EOF read
// errors into io.EOF for ReverseProxy's copier, while still recording the true
// error. Used only in join mode: finish() reports the truncation to every
// client, and stdlib need not panic over a response already being failed.
func (t *collapseBodyTracker) suppressReadErrors() {
	t.mu.Lock()
	t.suppress = true
	t.mu.Unlock()
}

// closeBody reclaims the concrete upstream response body (closing it unblocks
// a blocked Read) during abandonment or shutdown.
func (t *collapseBodyTracker) closeBody() {
	t.mu.Lock()
	body := t.body
	t.mu.Unlock()
	if body != nil {
		_ = body.Close()
	}
}

// record accounts one Read and keeps the first terminal non-EOF error. In
// suppress mode that error is presented to the copier as a clean EOF.
func (t *collapseBodyTracker) record(n int, err error) error {
	if err == nil {
		t.mu.Lock()
		t.n += int64(n)
		t.mu.Unlock()
		return nil
	}
	t.mu.Lock()
	t.n += int64(n)
	if !stderrors.Is(err, io.EOF) && t.readErr == nil {
		t.readErr = err
	}
	suppress := t.suppress
	t.mu.Unlock()
	if !stderrors.Is(err, io.EOF) && suppress {
		return io.EOF
	}
	return err
}

// outcome reports the owner-side failure for finish; declaredLen is the
// capture's parsed Content-Length and wins when it and the transport's
// disagreed. HEAD carries no response content and is exempt from length checks.
func (t *collapseBodyTracker) outcome(method string, declaredLen int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.readErr != nil {
		return t.readErr
	}
	declared := max(declaredLen, t.declared)
	if method != http.MethodHead && declared > 0 && t.n < declared {
		return io.ErrUnexpectedEOF
	}
	return nil
}

type trackedCollapseBody struct {
	t  *collapseBodyTracker
	rc io.ReadCloser
}

func (b *trackedCollapseBody) Read(p []byte) (int, error) {
	n, err := b.rc.Read(p)
	return n, b.t.record(n, err)
}

func (b *trackedCollapseBody) Close() error { return b.rc.Close() }
