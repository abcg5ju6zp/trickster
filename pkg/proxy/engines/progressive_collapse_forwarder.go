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
	"net/http"
	"sync"
	"sync/atomic"

	tbytes "github.com/trickstercache/trickster/v2/pkg/bytes"
	"github.com/trickstercache/trickster/v2/pkg/proxy/errors"
)

// IndexReader implements a reader to read data at a specific index into slice b
type IndexReader func(index uint64, b []byte) (int, error)

// ProgressiveCollapseForwarder accepts data written through the io.Writer interface, caches it and
// makes all the data written available to n readers. The readers can request data at index i,
// to which the PCF may block or return the data immediately.
//
// A PCF is the stable identity of ONE collapse round for one cache key: exactly
// one owner writes the upstream byte sequence, every joined client is bound to
// the same response headers and observes exactly that sequence. The terminal
// outcome is set exactly once — a clean Close or a single CloseWithError — so a
// truncated, abandoned or shut-down stream can never look like a complete
// object to a client or to anyone reusing GetBody.
type ProgressiveCollapseForwarder interface {
	AddClient(io.Writer) error
	AddClientContext(context.Context, io.Writer) error
	Write([]byte) (int, error)
	Close()
	CloseWithError(error)
	IndexRead(uint64, []byte) (int, error)
	WaitServerComplete()
	WaitAllComplete()
	GetBody() ([]byte, error)
	GetResp() *http.Response
}

type progressiveCollapseForwarder struct {
	resp           *http.Response
	rIndex         atomic.Uint64
	dataIndex      uint64
	dataLocker     sync.Mutex
	data           [][]byte
	dataStore      []byte
	maxSize        uint64
	fixed          bool
	readCond       *sync.Cond
	closeErr       error
	closeOnce      sync.Once
	serverReadDone atomic.Int32
	clientCond     *sync.Cond
	clientCount    atomic.Int32
	serverWaitCond *sync.Cond
}

// NewPCF returns a new instance of a ProgressiveCollapseForwarder. A negative
// contentLength means the object's size is unknown, and the store grows on
// demand up to maxSize; a positive contentLength preallocates exactly.
func NewPCF(resp *http.Response, contentLength, maxSize int64) ProgressiveCollapseForwarder {
	// Readers and the writer share the store safely because a reader may only
	// access chunk refs below rIndex, which the writer increments after a
	// chunk is fully written; chunk contents are immutable once committed.
	if contentLength == 0 || (contentLength < 0 && maxSize <= 0) {
		return nil
	}

	pcf := &progressiveCollapseForwarder{
		resp:           resp,
		readCond:       sync.NewCond(&sync.Mutex{}),
		clientCond:     sync.NewCond(&sync.Mutex{}),
		serverWaitCond: sync.NewCond(&sync.Mutex{}),
	}
	if contentLength > 0 {
		pcf.dataStore = make([]byte, contentLength)
		pcf.maxSize = uint64(contentLength)
		pcf.fixed = true
		pcf.data = make([][]byte, 0, (contentLength/HTTPBlockSize)+2)
	} else {
		pcf.maxSize = uint64(maxSize) // #nosec G115 -- guard above ensures maxSize > 0 on this branch
		pcf.data = make([][]byte, 0, 8)
	}
	return pcf
}

// AddClient adds an io.Writer client that is never interrupted by a request
// cancellation. See AddClientContext for the cancellable form used by joined
// HTTP clients.
func (pcf *progressiveCollapseForwarder) AddClient(w io.Writer) error {
	return pcf.AddClientContext(context.Background(), w)
}

// AddClientContext adds an io.Writer client to the ProgressiveCollapseForwarder.
// This client will read all the cached data and read from the live edge if
// caught up. It returns nil when the full object was delivered. Cancellation of
// ctx removes only THIS client's waiting relation: it neither fails the round
// nor interrupts the owner's upstream read other clients may still need. Any
// other error is the round's shared terminal failure (short body, stalled
// origin, max size, abandonment, shutdown) or this client's own write failure.
func (pcf *progressiveCollapseForwarder) AddClientContext(ctx context.Context, w io.Writer) error {
	pcf.clientCount.Add(1)
	defer func() {
		pcf.clientCount.Add(-1)
		pcf.clientCond.L.Lock()
		pcf.clientCond.Broadcast()
		pcf.clientCond.L.Unlock()
	}()
	var readIndex uint64
	var err error
	var n int
	bp := tbytes.GetCopyBuffer()
	defer tbytes.PutCopyBuffer(bp)
	buf := *bp
	// ctx cancellation wakes THIS waiter through the shared condition; every
	// waiter re-checks its own context, so a broadcast only lets it remove
	// itself — it can never publish a result for the round.
	stopWake := context.AfterFunc(ctx, func() {
		pcf.readCond.L.Lock()
		pcf.readCond.Broadcast()
		pcf.readCond.L.Unlock()
	})
	defer stopWake()
	for {
		n, err = pcf.indexReadContext(ctx, readIndex, buf)
		if n > 0 {
			if _, werr := w.Write(buf[0:n]); werr != nil {
				err = werr
				break
			}
			readIndex++
		}
		if err != nil {
			break
		}
	}
	if stderrors.Is(err, io.EOF) {
		return nil
	}
	return err
}

// WaitServerComplete blocks until the object has been retrieved from the origin server
func (pcf *progressiveCollapseForwarder) WaitServerComplete() {
	pcf.serverWaitCond.L.Lock()
	for pcf.serverReadDone.Load() == 0 {
		pcf.serverWaitCond.Wait()
	}
	pcf.serverWaitCond.L.Unlock()
}

// WaitAllComplete will wait till all clients have completed or timedout
func (pcf *progressiveCollapseForwarder) WaitAllComplete() {
	pcf.clientCond.L.Lock()
	for pcf.clientCount.Load() > 0 {
		pcf.clientCond.Wait()
	}
	pcf.clientCond.L.Unlock()
}

// GetBody returns the underlying body of the data written into a PCF. A PCF
// that terminated with an error hands back that error instead of bytes, so a
// partial sequence can never be stored as a reusable object.
func (pcf *progressiveCollapseForwarder) GetBody() ([]byte, error) {
	if pcf.serverReadDone.Load() == 0 {
		return nil, errors.ErrServerRequestNotCompleted
	}
	pcf.readCond.L.Lock()
	err := pcf.closeErr
	pcf.readCond.L.Unlock()
	if err != nil {
		return nil, err
	}
	pcf.dataLocker.Lock()
	defer pcf.dataLocker.Unlock()
	return pcf.dataStore[0:pcf.dataIndex], nil
}

// GetResp returns the response from the original request
func (pcf *progressiveCollapseForwarder) GetResp() *http.Response {
	return pcf.resp
}

// Write commits the data in b to the store in HTTPBlockSize chunks and makes
// each chunk visible to readers as it lands.
func (pcf *progressiveCollapseForwarder) Write(b []byte) (int, error) {
	if pcf.serverReadDone.Load() != 0 {
		return 0, io.ErrClosedPipe
	}
	var written int
	for len(b) > 0 {
		n := min(len(b), HTTPBlockSize)
		if err := pcf.writeChunk(b[:n]); err != nil {
			return written, err
		}
		written += n
		b = b[n:]
	}
	return written, nil
}

func (pcf *progressiveCollapseForwarder) writeChunk(b []byte) error {
	need := pcf.dataIndex + uint64(len(b))
	if need > pcf.maxSize {
		if pcf.fixed {
			return io.ErrShortWrite
		}
		return errors.ErrPCFMaxSizeExceeded
	}
	pcf.dataLocker.Lock()
	if need > uint64(len(pcf.dataStore)) {
		// grown copies are safe for readers: committed chunk refs point into
		// the old array, whose contents are immutable once written
		grown := max(uint64(len(pcf.dataStore))*2, uint64(HTTPBlockSize*4))
		grown = max(grown, need)
		grown = min(grown, pcf.maxSize)
		next := make([]byte, grown)
		copy(next, pcf.dataStore[:pcf.dataIndex])
		pcf.dataStore = next
	}
	ref := pcf.dataStore[pcf.dataIndex:need]
	copy(ref, b)
	pcf.data = append(pcf.data, ref)
	pcf.dataIndex = need
	pcf.dataLocker.Unlock()
	pcf.rIndex.Add(1)
	pcf.readCond.L.Lock()
	pcf.readCond.Broadcast()
	pcf.readCond.L.Unlock()
	return nil
}

// Close signals a successfully completed server response body to all waiters.
func (pcf *progressiveCollapseForwarder) Close() {
	pcf.finish(nil)
}

// CloseWithError terminates the stream, handing err to every attached client
// in place of a clean EOF so a failed or truncated upstream read is never
// mistaken for a complete object. A nil err is a normal Close. The first
// terminal outcome wins: a late success or a different failure from a racing
// goroutine can neither overwrite it nor republish the round.
func (pcf *progressiveCollapseForwarder) CloseWithError(err error) {
	pcf.finish(err)
}

// finish publishes the terminal outcome exactly once. The error is recorded
// before serverReadDone is set so no waiter can observe completion without it.
func (pcf *progressiveCollapseForwarder) finish(err error) {
	pcf.closeOnce.Do(func() {
		pcf.readCond.L.Lock()
		pcf.closeErr = err
		pcf.readCond.L.Unlock()
		pcf.serverReadDone.Store(1)
		pcf.serverWaitCond.L.Lock()
		pcf.serverWaitCond.Broadcast()
		pcf.serverWaitCond.L.Unlock()
		pcf.readCond.L.Lock()
		pcf.readCond.Broadcast()
		pcf.readCond.L.Unlock()
	})
}

// IndexRead will return the given index data if the read index is behind the PCF write index,
// else blocks and waits for the data to become available or for the server to finish.
func (pcf *progressiveCollapseForwarder) IndexRead(index uint64, b []byte) (int, error) {
	return pcf.indexReadContext(context.Background(), index, b)
}

// indexReadContext is the cancellable form of IndexRead: a cancelled ctx ends
// THIS wait only, leaving the shared byte stream and every other client alone.
func (pcf *progressiveCollapseForwarder) indexReadContext(ctx context.Context,
	index uint64, b []byte,
) (int, error) {
	pcf.readCond.L.Lock()
	for index >= pcf.rIndex.Load() {
		if pcf.serverReadDone.Load() != 0 {
			err := pcf.closeErr
			pcf.readCond.L.Unlock()
			if err != nil {
				return 0, err
			}
			return 0, io.EOF
		}
		// the shared round's terminal state is checked first, so a client
		// whose context ended at the same instant still learns the round's
		// authoritative conclusion rather than its own cancellation
		if ctxErr := ctx.Err(); ctxErr != nil {
			pcf.readCond.L.Unlock()
			return 0, ctxErr
		}
		pcf.readCond.Wait()
	}
	pcf.readCond.L.Unlock()
	var n int
	pcf.dataLocker.Lock()
	copy(b, pcf.data[index])
	n = len(pcf.data[index])
	pcf.dataLocker.Unlock()
	return n, nil
}
