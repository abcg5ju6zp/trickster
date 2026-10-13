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
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/trickstercache/trickster/v2/pkg/appinfo"
	bo "github.com/trickstercache/trickster/v2/pkg/backends/options"
	perrs "github.com/trickstercache/trickster/v2/pkg/proxy/errors"
	"github.com/trickstercache/trickster/v2/pkg/proxy/headers"
	po "github.com/trickstercache/trickster/v2/pkg/proxy/paths/options"
	"github.com/trickstercache/trickster/v2/pkg/proxy/request"
)

func TestCollapseEligible(t *testing.T) {
	get := httptest.NewRequest(http.MethodGet, "http://"+appinfo.Domain+"/", nil)
	authed := httptest.NewRequest(http.MethodGet, "http://"+appinfo.Domain+"/", nil)
	authed.Header.Set(headers.NameAuthorization, "Bearer x")
	keyed := po.New()
	keyed.CacheKeyHeaders = []string{"Accept-Encoding"}

	tests := []struct {
		name     string
		r        *http.Request
		code     int
		h        http.Header
		pc       *po.Options
		expected bool
	}{
		{"plain 200", get, 200, http.Header{}, po.New(), true},
		{"non-200", get, 206, http.Header{}, po.New(), false},
		{
			"set-cookie", get, 200,
			http.Header{headers.NameSetCookie: {"a=1"}},
			po.New(), false,
		},
		{
			"private", get, 200,
			http.Header{headers.NameCacheControl: {"private, max-age=60"}},
			po.New(), false,
		},
		{
			"no-store", get, 200,
			http.Header{headers.NameCacheControl: {"no-store"}},
			po.New(), false,
		},
		{
			"sse", get, 200,
			http.Header{headers.NameContentType: {"text/event-stream"}},
			po.New(), false,
		},
		{"authorized without public", authed, 200, http.Header{}, po.New(), false},
		{
			"authorized with public", authed, 200,
			http.Header{headers.NameCacheControl: {"public, max-age=60"}},
			po.New(), true,
		},
		{
			"authorized with s-maxage", authed, 200,
			http.Header{headers.NameCacheControl: {"s-maxage=30"}},
			po.New(), true,
		},
		{
			"vary star", get, 200,
			http.Header{headers.NameVary: {"*"}},
			po.New(), false,
		},
		{
			"vary unkeyed", get, 200,
			http.Header{headers.NameVary: {"Accept-Encoding"}},
			po.New(), false,
		},
		{
			"vary keyed", get, 200,
			http.Header{headers.NameVary: {"accept-encoding"}},
			keyed, true,
		},
		{
			"vary partially keyed", get, 200,
			http.Header{headers.NameVary: {"Accept-Encoding, Origin"}},
			keyed, false,
		},
		{
			"vary nil pathconfig", get, 200,
			http.Header{headers.NameVary: {"Accept-Encoding"}},
			nil, false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := collapseEligible(tc.r, tc.code, tc.h, tc.pc); got != tc.expected {
				t.Errorf("expected %v, got %v", tc.expected, got)
			}
		})
	}
}

// collapseHarness routes through CollapsedPassthrough to a real passthrough
// handler pointed at originURL, with Resources installed per request.
func collapseHarness(t *testing.T, originURL string, mutate ...func(*bo.Options, *po.Options),
) *httptest.Server {
	t.Helper()
	u, err := url.Parse(originURL)
	if err != nil {
		t.Fatal(err)
	}
	o := bo.New()
	o.Name = "test"
	o.Provider = "rp"
	o.Scheme = u.Scheme
	o.Host = u.Host
	o.PathPrefix = ""
	pc := po.New()
	for _, m := range mutate {
		m(o, pc)
	}
	client, err := NewTestClient("test", o, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	o.HTTPClient = client.HTTPClient()
	h := CollapsedPassthrough(NewPassthroughHandler(client))
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rsc := request.NewResources(o, pc, nil, nil, client, nil)
		h.ServeHTTP(w, request.SetResources(r, rsc))
	}))
	t.Cleanup(front.Close)
	return front
}

func awaitCollapseClients(t *testing.T, want int32) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var clients int32
		collapses.rounds.Range(func(_, value any) bool {
			rd := value.(*collapseRound)
			rd.cond.L.Lock()
			if pcf, ok := rd.pcf.(*progressiveCollapseForwarder); ok {
				clients += pcf.clientCount.Load()
			}
			rd.cond.L.Unlock()
			return true
		})
		if clients == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("collapse has %d clients, want %d", clients, want)
		}
		time.Sleep(time.Millisecond)
	}
}

// awaitRegistryDrained fails the test if a collapse round stays registered
// past the deadline, meaning an owner or waiter goroutine was not reclaimed.
func awaitRegistryDrained(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		count := 0
		collapses.rounds.Range(func(_, _ any) bool {
			count++
			return true
		})
		if count == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("collapse registry still holds %d round(s)", count)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestCollapsedPassthroughSharesOneFetch(t *testing.T) {
	var hits atomic.Int32
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	body := strings.Repeat("x", 4*HTTPBlockSize)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set(headers.NameContentLength, fmt.Sprintf("%d", len(body)))
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(body[:10]))
		http.NewResponseController(w).Flush()
		<-release // hold the stream open so followers can join
		w.Write([]byte(body[10:]))
	}))
	defer origin.Close()
	defer unblock()
	front := collapseHarness(t, origin.URL)

	const clients = 4
	var wg sync.WaitGroup
	results := make([]string, clients)
	errs := make([]error, clients)
	for i := range clients {
		wg.Go(func() {
			resp, err := http.Get(front.URL + "/obj")
			if err != nil {
				errs[i] = err
				return
			}
			defer resp.Body.Close()
			b, err := io.ReadAll(resp.Body)
			results[i] = string(b)
			errs[i] = err
		})
	}
	awaitCollapseClients(t, clients)
	unblock()
	wg.Wait()

	if got := hits.Load(); got != 1 {
		t.Errorf("expected exactly 1 upstream fetch, got %d", got)
	}
	for i := range clients {
		if errs[i] != nil {
			t.Errorf("client %d: %v", i, errs[i])
		}
		if results[i] != body {
			t.Errorf("client %d: body mismatch (len %d, want %d)", i, len(results[i]), len(body))
		}
	}
}

func TestCollapsedPassthroughSeparatesPreservedHosts(t *testing.T) {
	// a collapse joins requests for one object; where the origin sees the client's Host, requests
	// differing in it are different objects and lead their own fetches, and otherwise they are one
	run := func(t *testing.T, preserve bool, wantFetches int32, updates map[string]string) []string {
		var hits atomic.Int32
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			w.Header().Set(headers.NameContentLength, "8")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("hello, "))
			http.NewResponseController(w).Flush()
			<-release
			w.Write([]byte(r.Host[:1]))
		}))
		defer origin.Close()
		defer unblock()
		front := collapseHarness(t, origin.URL, func(o *bo.Options, pc *po.Options) {
			o.PreserveHost = preserve
			pc.RequestHeaders = updates
		})
		hosts := []string{"first.example.com", "second.example.com", "first.example.com"}
		results := make([]string, len(hosts))
		var wg sync.WaitGroup
		for i, host := range hosts {
			wg.Go(func() {
				req, err := http.NewRequest(http.MethodGet, front.URL+"/obj", nil)
				if err != nil {
					t.Error(err)
					return
				}
				req.Host = host
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Error(err)
					return
				}
				defer resp.Body.Close()
				b, _ := io.ReadAll(resp.Body)
				results[i] = string(b)
			})
		}
		awaitCollapseClients(t, int32(len(hosts)))
		unblock()
		wg.Wait()
		if got := hits.Load(); got != wantFetches {
			t.Errorf("expected %d upstream fetches, got %d", wantFetches, got)
		}
		return results
	}
	separate := func(t *testing.T, got []string) {
		t.Helper()
		want := []string{"hello, f", "hello, s", "hello, f"}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("client %d: got %q, want %q", i, got[i], want[i])
			}
		}
	}
	shared := func(t *testing.T, got []string, want string) {
		t.Helper()
		for i := range got {
			if got[i] != want {
				t.Errorf("client %d: got %q, want the one fetch's body %q", i, got[i], want)
			}
		}
	}
	t.Run("preserved", func(t *testing.T) { separate(t, run(t, true, 2, nil)) })
	t.Run("the origin's own host", func(t *testing.T) { shared(t, run(t, false, 1, nil), "hello, 1") })
	t.Run("preserved but set by the path", func(t *testing.T) {
		shared(t, run(t, true, 1, map[string]string{headers.NameHost: "x.example.com"}), "hello, x")
	})
	t.Run("preserved but deleted by the path", func(t *testing.T) {
		shared(t, run(t, true, 1, map[string]string{"-" + headers.NameHost: ""}), "hello, 1")
	})
	t.Run("preserved, the path's set empty", func(t *testing.T) {
		separate(t, run(t, true, 2, map[string]string{headers.NameHost: ""}))
	})
}

func TestCollapsedPassthroughRefusesPrivate(t *testing.T) {
	var hits atomic.Int32
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set(headers.NameCacheControl, "private")
		w.Write([]byte("secret"))
	}))
	defer origin.Close()
	front := collapseHarness(t, origin.URL)

	for range 2 {
		resp, err := http.Get(front.URL + "/priv")
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if string(b) != "secret" {
			t.Errorf("expected body delivery for ineligible response, got %q", b)
		}
	}
	if got := hits.Load(); got != 2 {
		t.Errorf("expected 2 independent fetches for a private response, got %d", got)
	}
}

func TestCollapsedPassthroughPostBypasses(t *testing.T) {
	var hits atomic.Int32
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Write([]byte("ok"))
	}))
	defer origin.Close()
	front := collapseHarness(t, origin.URL)

	for range 2 {
		resp, err := http.Post(front.URL+"/p", "text/plain", strings.NewReader("x"))
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	if got := hits.Load(); got != 2 {
		t.Errorf("POST must never collapse; expected 2 fetches, got %d", got)
	}
}

func TestCollapsedPassthroughUnknownLength(t *testing.T) {
	var hits atomic.Int32
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		// no Content-Length: chunked
		w.Write([]byte(strings.Repeat("y", 3*HTTPBlockSize)))
	}))
	defer origin.Close()
	front := collapseHarness(t, origin.URL)

	resp, err := http.Get(front.URL + "/chunked")
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(b) != 3*HTTPBlockSize {
		t.Errorf("expected full chunked body through collapse, got %d bytes", len(b))
	}
	if got := hits.Load(); got != 1 {
		t.Errorf("expected 1 fetch, got %d", got)
	}
}

func TestCollapsedPassthroughTruncationFansOut(t *testing.T) {
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(headers.NameContentLength, "4096")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("partial"))
		http.NewResponseController(w).Flush()
		<-release
		// close without sending the remaining bytes
		panic(http.ErrAbortHandler)
	}))
	defer origin.Close()
	defer unblock()
	front := collapseHarness(t, origin.URL)

	const clients = 2
	var wg sync.WaitGroup
	failures := make([]bool, clients)
	for i := range clients {
		wg.Go(func() {
			resp, err := http.Get(front.URL + "/trunc")
			if err != nil {
				failures[i] = true
				return
			}
			defer resp.Body.Close()
			if _, err := io.ReadAll(resp.Body); err != nil {
				failures[i] = true
			}
		})
	}
	awaitCollapseClients(t, clients)
	unblock()
	wg.Wait()

	for i, failed := range failures {
		if !failed {
			t.Errorf("client %d: truncated collapse presented as a complete response", i)
		}
	}
}

// streamGet issues a GET under ctx and streams the body. When abandonAfter is
// positive it cancels the request after that many bytes, simulating a client
// going away mid-body; the accumulated body and terminal read error return.
func streamGet(ctx context.Context, cancel context.CancelFunc, rawURL string,
	abandonAfter int,
) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var sb strings.Builder
	buf := make([]byte, 32)
	for {
		n, rerr := resp.Body.Read(buf)
		sb.Write(buf[:n])
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				return sb.String(), nil
			}
			return sb.String(), rerr
		}
		if abandonAfter > 0 && sb.Len() >= abandonAfter {
			cancel()
			_, _ = io.Copy(io.Discard, resp.Body)
			return sb.String(), nil
		}
	}
}

// TestCollapsedPassthroughLeaderDisconnectKeepsFollowers verifies the owner
// fetch is bound to the round, not to the first client: when the leader's
// connection goes away mid-body the detached fetch keeps running and every
// follower still receives the identical, complete response from one fetch.
func TestCollapsedPassthroughLeaderDisconnectKeepsFollowers(t *testing.T) {
	var hits atomic.Int32
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	body := strings.Repeat("x", 4*HTTPBlockSize)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set(headers.NameContentLength, fmt.Sprintf("%d", len(body)))
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(body[:10]))
		http.NewResponseController(w).Flush()
		<-release
		w.Write([]byte(body[10:]))
	}))
	defer origin.Close()
	defer unblock()
	front := collapseHarness(t, origin.URL)

	lctx, lcancel := context.WithCancel(context.Background())
	defer lcancel()
	leaderDone := make(chan struct{})
	go func() {
		_, _ = streamGet(lctx, lcancel, front.URL+"/obj", 0)
		close(leaderDone)
	}()
	awaitCollapseClients(t, 1)

	const followers = 3
	var wg sync.WaitGroup
	results := make([]string, followers)
	errs := make([]error, followers)
	for i := range followers {
		wg.Go(func() {
			resp, err := http.Get(front.URL + "/obj")
			if err != nil {
				errs[i] = err
				return
			}
			defer resp.Body.Close()
			b, err := io.ReadAll(resp.Body)
			results[i] = string(b)
			errs[i] = err
		})
	}
	awaitCollapseClients(t, followers+1)

	// leader goes away while followers still need the remaining bytes
	lcancel()
	select {
	case <-leaderDone:
	case <-time.After(2 * time.Second):
		t.Fatal("leader handler did not return after disconnect")
	}
	// only the leader's relation is removed; followers stay bound to the round
	awaitCollapseClients(t, followers)
	unblock()
	wg.Wait()

	if got := hits.Load(); got != 1 {
		t.Errorf("leader disconnect must not re-fetch; expected 1 upstream fetch, got %d", got)
	}
	for i := range followers {
		if errs[i] != nil {
			t.Errorf("follower %d: %v", i, errs[i])
		}
		if results[i] != body {
			t.Errorf("follower %d: body mismatch (len %d, want %d)", i, len(results[i]), len(body))
		}
	}
	awaitRegistryDrained(t)
}

// TestCollapsedPassthroughJoinerDisconnectKeepsOthers verifies a joiner's
// cancellation removes only its own waiting relation.
func TestCollapsedPassthroughJoinerDisconnectKeepsOthers(t *testing.T) {
	var hits atomic.Int32
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	body := strings.Repeat("y", 4*HTTPBlockSize)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set(headers.NameContentLength, fmt.Sprintf("%d", len(body)))
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(body[:8]))
		http.NewResponseController(w).Flush()
		<-release
		w.Write([]byte(body[8:]))
	}))
	defer origin.Close()
	defer unblock()
	front := collapseHarness(t, origin.URL)

	const clients = 3
	cancels := make([]context.CancelFunc, clients)
	ctxs := make([]context.Context, clients)
	for i := range clients {
		ctxs[i], cancels[i] = context.WithCancel(context.Background())
	}
	var wg sync.WaitGroup
	results := make([]string, clients)
	errs := make([]error, clients)
	for i := range clients {
		wg.Go(func() {
			results[i], errs[i] = streamGet(ctxs[i], cancels[i], front.URL+"/obj", 0)
		})
	}
	awaitCollapseClients(t, clients)

	// the middle joiner goes away; the other two keep waiting on the stream
	cancels[1]()
	time.Sleep(50 * time.Millisecond)
	unblock()
	wg.Wait()

	if got := hits.Load(); got != 1 {
		t.Errorf("expected 1 upstream fetch, got %d", got)
	}
	for i := range clients {
		if i == 1 {
			if errs[i] == nil {
				t.Error("cancelled joiner should observe its cancellation")
			}
			continue
		}
		if errs[i] != nil {
			t.Errorf("client %d must complete after a peer disconnected: %v", i, errs[i])
		}
		if results[i] != body {
			t.Errorf("client %d: body mismatch (len %d, want %d)", i, len(results[i]), len(body))
		}
	}
	awaitRegistryDrained(t)
}

// TestCollapsedPassthroughChunkedTruncationFailsAndReclaims covers the core
// failure mode: an unknown-length origin that dies mid-stream must not look
// like a complete object to any joined client, the round is unregistered, and
// the next request safely starts a new owner fetch that succeeds.
func TestCollapsedPassthroughChunkedTruncationFailsAndReclaims(t *testing.T) {
	var hits atomic.Int32
	var truncated atomic.Bool
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	full := strings.Repeat("z", 4*HTTPBlockSize)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		if !truncated.Load() {
			truncated.Store(true)
			// no Content-Length: chunked framing, closed mid-sequence
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("partial-chunk"))
			http.NewResponseController(w).Flush()
			<-release
			panic(http.ErrAbortHandler)
		}
		_, _ = io.WriteString(w, full)
	}))
	defer origin.Close()
	defer unblock()
	front := collapseHarness(t, origin.URL)

	const clients = 2
	var wg sync.WaitGroup
	failures := make([]bool, clients)
	for i := range clients {
		wg.Go(func() {
			resp, err := http.Get(front.URL + "/trunc")
			if err != nil {
				failures[i] = true
				return
			}
			defer resp.Body.Close()
			if _, err := io.ReadAll(resp.Body); err != nil {
				failures[i] = true
			}
		})
	}
	awaitCollapseClients(t, clients)
	unblock()
	wg.Wait()
	for i, failed := range failures {
		if !failed {
			t.Errorf("client %d: chunked truncation presented as a complete response", i)
		}
	}
	awaitRegistryDrained(t)

	// the dead round is gone: this request must claim a fresh round and win
	resp, err := http.Get(front.URL + "/trunc")
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || string(b) != full {
		t.Fatalf("re-claimed fetch failed: len=%d err=%v", len(b), err)
	}
	if got := hits.Load(); got != 2 {
		t.Errorf("expected a fresh owner fetch after truncation, got %d", got)
	}
}

// TestCollapsedPassthroughAbandonReclaimsOwnerFetch verifies that when every
// bound client leaves before completion, the owner's upstream read is actively
// interrupted (the origin observes request closure rather than draining to its
// timeout) and the registration is recycled.
func TestCollapsedPassthroughAbandonReclaimsOwnerFetch(t *testing.T) {
	var hits atomic.Int32
	var phase2 atomic.Bool
	ownerInterrupted := make(chan struct{}, 1)
	const partial = "partial-bytes-for-live-flush"
	full := strings.Repeat("q", 4*HTTPBlockSize)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if phase2.Load() {
			_, _ = io.WriteString(w, full)
			return
		}
		// chunked framing: the proxy flushes each write live, so the client
		// really has these bytes when it abandons the round
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, partial)
		http.NewResponseController(w).Flush()
		<-r.Context().Done() // unblocks only when the proxy closes THIS conn
		select {
		case ownerInterrupted <- struct{}{}:
		default:
		}
	}))
	defer origin.Close()
	front := collapseHarness(t, origin.URL)

	ctx, cancel := context.WithCancel(context.Background())
	_, err := streamGet(ctx, cancel, front.URL+"/obj", len(partial))
	if err != nil {
		t.Fatalf("streamGet abandonment: %v", err)
	}
	select {
	case <-ownerInterrupted:
	case <-time.After(3 * time.Second):
		t.Fatal("owner upstream read kept running after every client left")
	}
	awaitRegistryDrained(t)

	phase2.Store(true)
	resp, err := http.Get(front.URL + "/obj")
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || string(b) != full {
		t.Fatalf("fresh fetch after abandonment failed: len=%d err=%v", len(b), err)
	}
	if got := hits.Load(); got != 2 {
		t.Errorf("expected a fresh owner fetch after abandonment, got %d", got)
	}
}

// TestCollapsedPassthroughLeaderCancelBeforeHeaders exercises the window
// where the leader client leaves while the owner is still awaiting upstream
// headers: the leader frame must keep outliving the owner fetch (a bypass
// capture writes straight into its writer), and nothing may leak or deadlock.
func TestCollapsedPassthroughLeaderCancelBeforeHeaders(t *testing.T) {
	var hits atomic.Int32
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		<-release // headers held back past the leader's cancellation
		w.Header().Set(headers.NameCacheControl, "private")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "late-private-body")
	}))
	defer origin.Close()
	defer unblock()
	front := collapseHarness(t, origin.URL)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := streamGet(ctx, cancel, front.URL+"/late", 0)
		done <- err
	}()
	// wait for the round to exist with the leader bound and awaiting headers
	deadline := time.Now().Add(2 * time.Second)
	for {
		count := 0
		collapses.rounds.Range(func(_, _ any) bool { count++; return true })
		if count > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("leader collapse round never registered")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled leader handler stayed stuck before headers")
	}
	// owner now resolves bypass and writes into the (gone) leader writer
	unblock()
	awaitRegistryDrained(t)
	if got := hits.Load(); got != 1 {
		t.Errorf("expected exactly 1 owner fetch, got %d", got)
	}
}

// TestCollapseRegistryShutdownReclaimsRounds verifies service shutdown fails
// in-flight rounds with a distinguishable outcome, drains the registry, and new
// requests take the normal single-request path.
func TestCollapseRegistryShutdownReclaimsRounds(t *testing.T) {
	prev := collapses
	reg := newCollapseRegistry()
	collapses = reg
	t.Cleanup(func() { collapses = prev })

	var hits atomic.Int32
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	body := strings.Repeat("s", 4*HTTPBlockSize)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set(headers.NameContentLength, fmt.Sprintf("%d", len(body)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body[:12]))
		http.NewResponseController(w).Flush()
		<-release
		_, _ = w.Write([]byte(body[12:]))
	}))
	defer origin.Close()
	defer unblock()
	front := collapseHarness(t, origin.URL)

	const clients = 2
	var wg sync.WaitGroup
	failures := make([]bool, clients)
	for i := range clients {
		wg.Go(func() {
			resp, err := http.Get(front.URL + "/obj")
			if err != nil {
				failures[i] = true
				return
			}
			defer resp.Body.Close()
			if _, err := io.ReadAll(resp.Body); err != nil {
				failures[i] = true
			}
		})
	}
	awaitCollapseClients(t, clients)

	reg.shutdown()
	if !reg.shuttingDown() {
		t.Fatal("registry must report shutting down")
	}
	wg.Wait()
	for i, failed := range failures {
		if !failed {
			t.Errorf("client %d: shutdown looked like a clean EOF", i)
		}
	}
	awaitRegistryDrained(t)

	// blocked owner is released; subsequent requests bypass the dead registry
	unblock()
	resp, err := http.Get(front.URL + "/obj")
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || string(b) != body {
		t.Fatalf("post-shutdown single-request path failed: len=%d err=%v", len(b), err)
	}
	if got := hits.Load(); got != 2 {
		t.Errorf("expected an independent fetch after shutdown, got %d", got)
	}
}

// TestCollapseRoundIdentityGuardsLateOwner verifies an owner goroutine from a
// reclaimed round can neither delete nor close a successor round for the same
// key when it reaches its terminal step late.
func TestCollapseRoundIdentityGuardsLateOwner(t *testing.T) {
	reg := newCollapseRegistry()
	mkRound := func(id uint64) *collapseRound {
		return &collapseRound{id: id, key: "k", reg: reg, cond: sync.NewCond(&sync.Mutex{})}
	}
	dead := mkRound(1)
	reg.rounds.Store("k", dead)
	successor := mkRound(2)
	reg.rounds.Store("k", successor) // the successor round replaced the dead one

	dead.addParty()
	dead.leave() // zero parties: abandon, but CompareAndDelete must miss successor
	if cur, _ := reg.rounds.Load("k"); cur.(*collapseRound) != successor {
		t.Fatal("abandon of a dead round removed a successor registration")
	}
	if !dead.abandoned {
		t.Fatal("dead round should be marked abandoned")
	}

	// the late owner goroutine now reaches its finish step on the dead round
	cw := &collapseCapture{
		rd:  dead,
		req: httptest.NewRequest(http.MethodGet, "http://example.com/k", nil),
	}
	cw.finish()
	if cur, _ := reg.rounds.Load("k"); cur.(*collapseRound) != successor {
		t.Fatal("late owner finish removed the successor registration")
	}
	if !dead.ownerDone {
		t.Fatal("late finish should still retire the dead round state")
	}
	if successor.abandoned || successor.ownerDone {
		t.Fatal("the successor round must be untouched by the late owner")
	}

	// the successor's own owner finishes normally and removes exactly itself
	cw2 := &collapseCapture{
		rd:  successor,
		req: httptest.NewRequest(http.MethodGet, "http://example.com/k", nil),
	}
	cw2.finish()
	if _, ok := reg.rounds.Load("k"); ok {
		t.Fatal("owner finish did not unregister its own round")
	}
	if !successor.bypass {
		t.Fatal("a pre-header finish releases waiters to independent fetches")
	}
}

// TestCollapseBodyTrackerOutcomes verifies the read-side authority distinguishes
// a clean whole body, a mid-stream read failure, and a short declared body.
func TestCollapseBodyTrackerOutcomes(t *testing.T) {
	rc := io.NopCloser(strings.NewReader("0123456789"))
	tr := newCollapseBodyTracker()
	wrapped := tr.track(rc, 10)
	b, _ := io.ReadAll(wrapped)
	if string(b) != "0123456789" {
		t.Fatalf("tracker altered bytes: %q", b)
	}
	if err := tr.outcome(http.MethodGet, 10); err != nil {
		t.Errorf("whole body should succeed: %v", err)
	}

	failing := io.NopCloser(&errorAfterReader{data: []byte("partial")})
	tr2 := newCollapseBodyTracker()
	w2 := tr2.track(failing, -1)
	_, _ = io.Copy(io.Discard, w2)
	if err := tr2.outcome(http.MethodGet, -1); err == nil {
		t.Error("a non-EOF read error on chunked framing must fail the round")
	}

	// suppress mode hands the copier a clean EOF while keeping the true error
	oneByte := make([]byte, 100)
	tr3 := newCollapseBodyTracker()
	w3 := tr3.track(io.NopCloser(&errorAfterReader{data: []byte("partial")}), -1)
	tr3.suppressReadErrors()
	var total int
	var rerr error
	for {
		n, e := w3.Read(oneByte)
		total += n
		if e != nil {
			rerr = e
			break
		}
	}
	if !errors.Is(rerr, io.EOF) || total != len("partial") {
		t.Fatalf("suppressed read error should look like EOF to the copier (n=%d), got %v", total, rerr)
	}
	if err := tr3.outcome(http.MethodGet, -1); err == nil {
		t.Error("suppression must not erase the authoritative failure")
	}
}

type errorAfterReader struct {
	data []byte
	sent bool
}

func (r *errorAfterReader) Read(p []byte) (int, error) {
	if !r.sent {
		r.sent = true
		return copy(p, r.data), nil
	}
	return 0, perrs.ErrOriginStalled
}

func TestPCFGrowthCapAborts(t *testing.T) {
	resp := &http.Response{}
	pcf := NewPCF(resp, -1, int64(HTTPBlockSize))
	if pcf == nil {
		t.Fatal("expected growable PCF")
	}
	if _, err := pcf.Write(make([]byte, HTTPBlockSize)); err != nil {
		t.Fatalf("write within cap failed: %v", err)
	}
	if _, err := pcf.Write([]byte("overflow")); err == nil {
		t.Error("expected an error writing past the growth cap")
	}
}

func TestPCFCloseWithErrorReachesClients(t *testing.T) {
	resp := &http.Response{}
	pcf := NewPCF(resp, -1, int64(HTTPBlockSize*4))
	pcf.Write([]byte("partial"))
	done := make(chan error, 1)
	go func() {
		done <- pcf.AddClient(io.Discard)
	}()
	pcf.CloseWithError(io.ErrUnexpectedEOF)
	select {
	case err := <-done:
		if err != io.ErrUnexpectedEOF {
			t.Errorf("expected ErrUnexpectedEOF to reach the client, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("client never released after CloseWithError")
	}
}
