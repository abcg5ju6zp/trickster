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

package index

import (
	"context"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/trickstercache/trickster/v2/pkg/cache/blob"
	"github.com/trickstercache/trickster/v2/pkg/cache/blob/blobtest"
	"github.com/trickstercache/trickster/v2/pkg/parsing/timeconv"

	"github.com/stretchr/testify/require"
)

// while the index cannot vouch for the cache, the reaper evicts nothing; once a sweep
// completes, the same objects are reaped
func TestReapWaitsForAConfirmedIndex(t *testing.T) {
	s := blobtest.NewMemStore()
	s.ScanErr = errFault
	o := idleOpts()
	o.ScanInterval = timeconv.Duration(time.Hour)
	idx := openSweptIndex(t, s, o)
	require.False(t, idx.confirmed.Load())
	require.NoError(t, idx.Store("soon", []byte("1"), time.Nanosecond))
	require.NoError(t, idx.Store("kept", []byte("12"), time.Hour))
	time.Sleep(time.Millisecond)

	idx.reap()
	requireTotals(t, idx, 2, 3)
	require.Equal(t, 2, s.Len())

	// stop the workers, so the successful sweep and reap that follow are deterministic
	idx.crash()
	s.ScanErr = nil
	idx.sweep(context.Background())
	require.True(t, idx.confirmed.Load())
	idx.reapAt(time.Now().Add(time.Minute).UnixNano())
	require.Equal(t, []string{"kept"}, idx.Keys())
}

// a failed sweep keeps what it had already found, and still does not confirm the index
func TestFailedSweepKeepsProgressAndDoubt(t *testing.T) {
	s := blobtest.NewMemStore()
	idx := openIndex(t, s, idleOpts())
	idx.scanner = blob.NewClient(s, testName, testProvider)
	require.NoError(t, idx.Store("found", []byte("12"), time.Hour))

	s.ScanErr = errFault
	idx.confirmed.Store(false)
	idx.sweep(t.Context())
	require.False(t, idx.confirmed.Load())
	requireTotals(t, idx, 1, 2)

	// a scan that reaches the end confirms, even one that adopts nothing more
	s.ScanErr = nil
	idx.sweep(t.Context())
	require.True(t, idx.confirmed.Load())
}

// an eviction the cache's response leaves in doubt gives the index its entries back, keeps
// the counts, leaves the objects eligible to be chosen again, and says it freed nothing
func TestEvictRestoresAfterDeleteFailure(t *testing.T) {
	s := blobtest.NewMemStore()
	idx := openIndex(t, s, idleOpts())
	require.NoError(t, idx.Store("a", []byte("1"), time.Hour))
	require.NoError(t, idx.Store("bb", []byte("22"), time.Hour))
	time.Sleep(time.Millisecond)
	since := time.Now()
	oa, _ := idx.Object("a")
	oa.evicting.Store(true)

	s.DeleteErr = errFault
	idx.evict(reasonBytes, since, []string{"a", "bb"})
	requireTotals(t, idx, 2, 3)
	require.Equal(t, 2, s.Len(), "the cache was not told of a certain deletion")
	for _, key := range []string{"a", "bb"} {
		o, listed := idx.Object(key)
		require.True(t, listed, key)
		require.False(t, o.evicting.Load(), "restored entries can be chosen again")
	}

	// the compensating records survive a restart: the remove is answered by a later add
	idx.flushOnce()
	idx.crash()
	reopened := openIndex(t, s, idleOpts())
	requireTotals(t, reopened, 2, 3)

	// a successful retry removes them
	s.DeleteErr = nil
	reopened.evict(reasonBytes, time.Now(), []string{"a", "bb"})
	requireTotals(t, reopened, 0, 0)
	require.Zero(t, s.Len())
}

// a relist never moves an object back past facts newer than the ones that were evicted
func TestRelistKeepsNewerFacts(t *testing.T) {
	s := blobtest.NewMemStore()
	idx := openIndex(t, s, idleOpts())
	require.NoError(t, idx.Store("k", []byte("old"), time.Hour))
	old, _ := idx.Object("k")
	entry := evictedEntry{o: old, size: old.Size(), transient: false}
	// the object is written again, with a larger body, before the relist runs
	require.NoError(t, idx.Store("k", []byte("brand new"), time.Hour))
	unlock := idx.lockKeys([]string{"k"})
	idx.relistLocked([]string{"k"}, []evictedEntry{entry})
	unlock()
	requireTotals(t, idx, 1, 9)
	o, _ := idx.Object("k")
	require.Equal(t, int64(9), o.Size())
}

// facts a sweep finds move the index forward by last write, and never back
func TestSweepAdoptsByLastWrite(t *testing.T) {
	s := blobtest.NewMemStore()
	idx := openIndex(t, s, idleOpts())
	idx.scanner = blob.NewClient(s, testName, testProvider)

	// a small frame, adopted by the first sweep
	plant(t, s, "k", []byte("1"), time.Hour)
	idx.sweep(t.Context())
	o, _ := idx.Object("k")
	require.Equal(t, int64(1), o.Size())
	accessed := o.LastAccess()

	// the index learns newer facts than what a later sweep can see on the medium
	indexTime := time.Now()
	newExpiry := indexTime.Add(10 * time.Hour)
	o.size.Store(5)
	o.expiration.Store(newExpiry.UnixNano())
	o.lastWrite.Store(indexTime.UnixNano())
	idx.cacheSize.Add(4)
	// the medium still shows the old frame, so the sweep must change nothing
	idx.sweep(t.Context())
	o, _ = idx.Object("k")
	require.Equal(t, int64(5), o.Size(), "older disk facts never downdate the size")
	require.Equal(t, newExpiry.UnixNano(), o.expiration.Load(), "or the expiration")
	require.Equal(t, accessed, o.LastAccess(), "a sweep is not an access")
	requireTotals(t, idx, 1, 5)

	// newer disk facts do win
	time.Sleep(time.Millisecond)
	plant(t, s, "k", []byte("seven77"), time.Hour)
	idx.sweep(t.Context())
	o, _ = idx.Object("k")
	require.Equal(t, int64(7), o.Size())
	requireTotals(t, idx, 1, 7)
}

// a newer snapshot that is damaged is abandoned for the newest intact one, and the journals
// since complete it
func TestReopenFallsBackToAnIntactSnapshot(t *testing.T) {
	s := blobtest.NewMemStore()
	idx := openIndex(t, s, idleOpts())
	require.NoError(t, idx.Store("a", []byte("1"), time.Hour))
	require.NoError(t, idx.journal.compact(time.Now(), idx.each)) // snapshot gen 2: a
	require.NoError(t, idx.Store("b", []byte("22"), time.Hour))
	idx.flushOnce() // journal gen 2: b
	// publish snapshot gen 3 (a,b) without reaping the generations before, as a removal
	// whose response was uncertain right after publication would leave them
	idx.journal.store = failingMeta{MemStore: s, remove: true}
	require.NoError(t, idx.journal.compact(time.Now(), idx.each))
	idx.journal.store = s
	require.Contains(t, metaNames(t, s), generationName(snapshotPrefix, 2))
	require.Contains(t, metaNames(t, s), generationName(journalPrefix, 2))

	// rot damages the newest snapshot after publication
	damaged := generationName(snapshotPrefix, 3)
	b := readMeta(t, s, damaged)
	b[len(b)-1] ^= 1
	s.SetMeta(damaged, b)

	reopened := openIndex(t, s, idleOpts())
	requireTotals(t, reopened, 2, 3)
	keys := reopened.Keys()
	slices.Sort(keys)
	require.Equal(t, []string{"a", "b"}, keys)
	require.True(t, reopened.sweepDue, "without the clean mark the untidy close still asks for a sweep")
}

// every snapshot damaged means nothing is adopted, and the cache is swept anew
func TestReopenWithAllSnapshotsDamaged(t *testing.T) {
	s := blobtest.NewMemStore()
	s.ScanErr = errFault // hold the startup sweep until the doubtful state has been checked
	idx := openSweptIndex(t, s, idleOpts())
	require.False(t, idx.confirmed.Load())
	require.NoError(t, idx.Store("a", []byte("1"), time.Hour))
	require.NoError(t, idx.journal.compact(time.Now(), idx.each))
	var name string
	for _, n := range metaNames(t, s) {
		if _, ok := generationOf(n, snapshotPrefix); ok {
			name = n
		}
	}
	require.NotEmpty(t, name)
	b := readMeta(t, s, name)
	b[len(b)-1] ^= 1
	s.SetMeta(name, b)
	idx.crash()

	reopened := openSweptIndex(t, s, idleOpts())
	require.True(t, reopened.sweepDue)
	require.False(t, reopened.confirmed.Load())
	requireTotals(t, reopened, 0, 0)

	reopened.crash()
	s.ScanErr = nil
	reopened.sweep(context.Background())
	require.True(t, reopened.confirmed.Load())
	requireTotals(t, reopened, 1, 1)
}

// the clean marker vouches only for the generation it names: one left by an older close
// cannot vouch for writes made later
func TestCleanMarkerIsGenerationScoped(t *testing.T) {
	s := blobtest.NewMemStore()
	idx := openIndex(t, s, idleOpts())
	require.NoError(t, idx.Store("a", []byte("1"), time.Hour))
	require.NoError(t, idx.Close())
	require.Contains(t, metaNames(t, s), cleanMarker)

	// a later, crashed session left a journal past the marked generation
	rec := appendRecord(nil, &record{op: opAdd, key: "later", size: 1, lastWrite: time.Now().UnixNano()})
	rec = appendMark(rec, time.Now())
	require.NoError(t, s.AppendMeta(generationName(journalPrefix, 2), rec))

	reopened := openIndex(t, s, idleOpts())
	require.True(t, reopened.sweepDue, "a stale mark cannot vouch for the later generation")
	require.Equal(t, []string{"a", "later"}, sortedKeys(reopened))

	// a marker that carries no generation vouches for nothing
	s2 := blobtest.NewMemStore()
	i2 := openIndex(t, s2, idleOpts())
	require.NoError(t, i2.Store("a", []byte("1"), time.Hour))
	i2.flushOnce()
	s2.SetMeta(cleanMarker, []byte("not-a-generation"))
	i2.crash()
	r2 := openIndex(t, s2, idleOpts())
	require.True(t, r2.sweepDue)
	require.Equal(t, []string{"a"}, r2.Keys())
}

// overlapping sweeps must not take one another's marks for objects the cache lost
func TestConcurrentSweepsDoNotDropObjects(t *testing.T) {
	s := blobtest.NewMemStore()
	idx := openIndex(t, s, idleOpts())
	idx.scanner = blob.NewClient(s, testName, testProvider)
	for i := range 20 {
		plant(t, s, "key-"+strconv.Itoa(i), []byte("1"), time.Hour)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() { idx.sweep(t.Context()) })
	}
	wg.Wait()
	requireTotals(t, idx, 20, 20)
	require.True(t, idx.confirmed.Load())
}

// a sweep already in progress makes another return at once
func TestSweepSkipsWhenOneIsRunning(t *testing.T) {
	s := blobtest.NewMemStore()
	idx := openIndex(t, s, idleOpts())
	idx.scanner = blob.NewClient(s, testName, testProvider)
	idx.sweepMu.Lock()
	idx.sweep(t.Context())
	idx.sweepMu.Unlock()
	require.Zero(t, idx.sweeps.Load())
}

// blocks the first AppendMeta until released, to stand in for a worker stuck in storage I/O
type blockingMeta struct {
	*blobtest.MemStore
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

func (b *blockingMeta) AppendMeta(string, []byte) error {
	b.once.Do(func() { close(b.started) })
	<-b.release
	return errFault
}

// a close whose workers do not stop in time persists nothing more and leaves a state the
// next start sorts out, rather than hanging or writing over the stuck worker
func TestCloseTimesOutAndLeavesRecoverableState(t *testing.T) {
	old := closeWorkerTimeout
	closeWorkerTimeout = 50 * time.Millisecond
	t.Cleanup(func() { closeWorkerTimeout = old })

	s := blobtest.NewMemStore()
	blocker := &blockingMeta{MemStore: s, started: make(chan struct{}), release: make(chan struct{})}
	o := idleOpts()
	o.FlushInterval = timeconv.Duration(time.Millisecond)
	idx := openIndex(t, s, o)
	idx.journal.store = blocker
	require.NoError(t, idx.Store("k", []byte("1"), time.Hour))
	select {
	case <-blocker.started:
	case <-time.After(testTimeout):
		t.Fatal("the flusher did not reach the store")
	}

	require.NoError(t, idx.Close())
	require.NotContains(t, metaNames(t, s), cleanMarker, "no clean mark is published over a stuck worker")

	close(blocker.release)
	require.Eventually(t, idx.flusherExited.Load, testTimeout, time.Millisecond)

	// the blocked append came to nothing; the next start trusts no index, and the sweep
	// finds the object that was written to the cache
	reopened := openIndex(t, s, idleOpts())
	reopened.scanner = blob.NewClient(s, testName, testProvider)
	require.True(t, reopened.sweepDue)
	requireTotals(t, reopened, 0, 0)
	_, held := s.Frame("k")
	require.True(t, held)
	reopened.sweep(t.Context())
	requireTotals(t, reopened, 1, 1)
}
