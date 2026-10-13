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
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/trickstercache/trickster/v2/pkg/cache/blob"
)

const (
	opAdd    = 1
	opRemove = 2
	// a mark record carries only the time it was written, in its last write field
	opMark = 3

	// a record is its head, the key, and a checksum of both
	recordHeadLen     = 35
	recordChecksumLen = 4
	recordOverhead    = recordHeadLen + recordChecksumLen
	maxRecordKeyLen   = 4096

	offRecordKeyLen     = 1
	offRecordSize       = 3
	offRecordExpiration = 11
	offRecordLastWrite  = 19
	offRecordLastAccess = 27

	snapshotPrefix = "index."
	journalPrefix  = "journal."
	cleanMarker    = "clean"
	generationLen  = 20
	decimal        = 10

	// transientTTL is the lifetime under which an object is gone too soon to be worth persisting
	transientTTL = time.Minute
	// the journal is compacted once it outgrows this share of the snapshot, and this size
	compactionRatio    = 2
	compactionMinBytes = 1 << 20
	// maxJournalBuffer bounds the records held for the next flush, when flushes keep failing
	maxJournalBuffer = 64 << 20
	snapshotBuffer   = 1 << 20
)

var (
	recordTable      = crc32.MakeTable(crc32.Castagnoli)
	errCorruptRecord = errors.New("corrupt index record")
	// errDamagedIndex is returned when no snapshot of a persisted index can be read whole,
	// so nothing it held is to be trusted and the cache must be swept anew
	errDamagedIndex = errors.New("persisted cache index is damaged")
)

type record struct {
	op                                      byte
	key                                     string
	size, expiration, lastWrite, lastAccess int64
}

func appendRecord(dst []byte, r *record) []byte {
	start := len(dst)
	var head [recordHeadLen]byte
	head[0] = r.op
	// #nosec G115 -- a key is bounded by maxRecordKeyLen before it is ever stored
	binary.LittleEndian.PutUint16(head[offRecordKeyLen:], uint16(len(r.key)))
	// #nosec G115 -- each value keeps its two's-complement bit pattern, and is decoded the same way
	binary.LittleEndian.PutUint64(head[offRecordSize:], uint64(r.size))
	// #nosec G115 -- see the size above
	binary.LittleEndian.PutUint64(head[offRecordExpiration:], uint64(r.expiration))
	// #nosec G115 -- see the size above
	binary.LittleEndian.PutUint64(head[offRecordLastWrite:], uint64(r.lastWrite))
	// #nosec G115 -- see the size above
	binary.LittleEndian.PutUint64(head[offRecordLastAccess:], uint64(r.lastAccess))
	dst = append(append(dst, head[:]...), r.key...)
	return binary.LittleEndian.AppendUint32(dst, crc32.Checksum(dst[start:], recordTable))
}

func appendObject(dst []byte, o *Object) []byte {
	return appendRecord(dst, &record{
		op: opAdd, key: o.Key, size: o.size.Load(), expiration: o.expiration.Load(),
		lastWrite: o.lastWrite.Load(), lastAccess: o.lastAccess.Load(),
	})
}

func appendMark(dst []byte, now time.Time) []byte {
	return appendRecord(dst, &record{op: opMark, lastWrite: now.UnixNano()})
}

// returns io.EOF at the end of the records, and errCorruptRecord for one that is torn or
// damaged
func readRecord(r io.Reader, buf []byte) (record, error) {
	var rec record
	head := buf[:recordHeadLen]
	if n, err := io.ReadFull(r, head); err != nil {
		if n == 0 && errors.Is(err, io.EOF) {
			return rec, io.EOF
		}
		return rec, errCorruptRecord
	}
	keyLen := int(binary.LittleEndian.Uint16(head[offRecordKeyLen:]))
	if keyLen > maxRecordKeyLen || head[0] < opAdd || head[0] > opMark {
		return rec, errCorruptRecord
	}
	rest := buf[recordHeadLen : recordOverhead+keyLen]
	if _, err := io.ReadFull(r, rest); err != nil {
		return rec, errCorruptRecord
	}
	sum := crc32.Update(crc32.Checksum(head, recordTable), recordTable, rest[:keyLen])
	if sum != binary.LittleEndian.Uint32(rest[keyLen:]) {
		return rec, errCorruptRecord
	}
	rec.op = head[0]
	rec.key = string(rest[:keyLen])
	// #nosec G115 -- reverses the bit-preserving conversions made when encoding
	rec.size = int64(binary.LittleEndian.Uint64(head[offRecordSize:]))
	// #nosec G115 -- see the size above
	rec.expiration = int64(binary.LittleEndian.Uint64(head[offRecordExpiration:]))
	// #nosec G115 -- see the size above
	rec.lastWrite = int64(binary.LittleEndian.Uint64(head[offRecordLastWrite:]))
	// #nosec G115 -- see the size above
	rec.lastAccess = int64(binary.LittleEndian.Uint64(head[offRecordLastAccess:]))
	return rec, nil
}

// generations are padded so that the names of a kind sort in the order of their generations
func generationName(prefix string, generation uint64) string {
	n := strconv.FormatUint(generation, decimal)
	return prefix + strings.Repeat("0", generationLen-len(n)) + n
}

func generationOf(name, prefix string) (uint64, bool) {
	n, ok := strings.CutPrefix(name, prefix)
	if !ok || len(n) != generationLen {
		return 0, false
	}
	generation, err := strconv.ParseUint(n, decimal, 64)
	return generation, err == nil
}

// a snapshot of the index and the changes made since, so that a change costs a small append
// and not a rewrite
type journal struct {
	store blob.MetaStore

	// opMu serializes flush and compact publication runs, so a close that has given up
	// waiting on a stopped worker can never interleave its own writes with the worker's
	opMu sync.Mutex

	// mu guards the records waiting to be flushed and the generation they are bound for
	mu         sync.Mutex
	pending    []byte
	spare      []byte
	generation uint64
	// lossy marks a journal that is missing records, and so must be replaced by a snapshot
	lossy bool

	// the rest are the flusher's alone, apart from opMu
	journalBytes  int64
	snapshotBytes int64
}

// a nil journal records nothing
func (j *journal) add(o *Object) {
	if j == nil {
		return
	}
	j.mu.Lock()
	if len(j.pending) < maxJournalBuffer {
		j.pending = appendObject(j.pending, o)
	} else {
		j.lossy = true
	}
	j.mu.Unlock()
}

// a nil journal records nothing
func (j *journal) remove(cacheKey string) {
	if j == nil {
		return
	}
	j.mu.Lock()
	if len(j.pending) < maxJournalBuffer {
		j.pending = appendRecord(j.pending, &record{op: opRemove, key: cacheKey})
	} else {
		j.lossy = true
	}
	j.mu.Unlock()
}

// returns what waits to be flushed, its generation, and whether records were lost; advance
// starts a new generation first
func (j *journal) take(advance bool) ([]byte, uint64, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if advance {
		j.generation++
	}
	pending := j.pending
	j.pending, j.spare = j.spare[:0], nil
	lossy := j.lossy
	j.lossy = false
	return pending, j.generation, lossy
}

func (j *journal) recycle(b []byte) {
	if cap(b) > maxJournalBuffer {
		return
	}
	j.mu.Lock()
	j.spare = b[:0]
	j.mu.Unlock()
}

func (j *journal) markLossy() {
	j.mu.Lock()
	j.lossy = true
	j.mu.Unlock()
}

// reports whether the journal has grown enough, or lost enough, to be replaced by a snapshot.
//
// The records and their mark land as one append, so an observer either sees every one of
// them with the mark that closes them, or sees none past the last whole record. A response
// that fails is taken to mean the append may have landed torn or whole, so what it held is
// not to be relied on and a snapshot is called for.
func (j *journal) flush(now time.Time) (bool, error) {
	j.opMu.Lock()
	defer j.opMu.Unlock()
	pending, generation, lossy := j.take(false)
	var err error
	if len(pending) > 0 {
		pending = appendMark(pending, now)
		if err = j.store.AppendMeta(generationName(journalPrefix, generation), pending); err == nil {
			j.journalBytes += int64(len(pending))
		} else {
			lossy = true
		}
	}
	j.recycle(pending)
	if lossy {
		j.markLossy()
	}
	return lossy || j.journalBytes > max(compactionMinBytes, j.snapshotBytes/compactionRatio), err
}

// a snapshot of the objects each yields, in a new generation, after which the generations
// before are removed. Publication is ordered: the snapshot first, whole and under its final
// name, and only then the generations it supersedes, whose removal failing changes nothing
// the snapshot does not already establish
func (j *journal) compact(now time.Time, each func(yield func(o *Object) bool)) error {
	j.opMu.Lock()
	defer j.opMu.Unlock()
	// records made from here on are bound for the new generation's journal; what they
	// record may be in the snapshot as well, and replaying them over it changes nothing
	pending, generation, _ := j.take(true)
	if len(pending) > 0 {
		// these were bound for the generation before, and the snapshot will hold what they record
		j.recycle(pending)
	}
	if err := j.publishSnapshot(generation, now, each); err != nil {
		// the generations before this one still stand, and are replayed along with its journal
		j.markLossy()
		return err
	}
	// old generations lingering after a failed removal are dead weight, never evidence:
	// loading starts from the snapshot that was just published, and the next compaction
	// lists and removes them again
	_ = j.removeBefore(generation)
	return nil
}

// writes the snapshot whole and publishes it only once its writer closed without error, so
// a snapshot that is torn by a crash is never visible under its final name
func (j *journal) publishSnapshot(generation uint64, now time.Time, each func(yield func(o *Object) bool)) error {
	w, err := j.store.CreateMeta(generationName(snapshotPrefix, generation))
	if err != nil {
		return err
	}
	bw := bufio.NewWriterSize(w, snapshotBuffer)
	scratch := appendMark(make([]byte, 0, recordOverhead+maxRecordKeyLen), now)
	written := int64(len(scratch))
	if _, err = bw.Write(scratch); err == nil {
		nowNano := now.UnixNano()
		each(func(o *Object) bool {
			if err != nil || o.transient.Load() || o.expired(nowNano) {
				return err == nil
			}
			scratch = appendObject(scratch[:0], o)
			written += int64(len(scratch))
			_, err = bw.Write(scratch)
			return err == nil
		})
	}
	if err == nil {
		err = bw.Flush()
	}
	if cerr := w.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	j.snapshotBytes, j.journalBytes = written, 0
	return nil
}

func (j *journal) removeBefore(generation uint64) error {
	names, err := j.store.ListMeta()
	if err != nil {
		return err
	}
	stale := names[:0]
	for _, name := range names {
		for _, prefix := range [...]string{snapshotPrefix, journalPrefix} {
			if g, ok := generationOf(name, prefix); ok && g < generation {
				stale = append(stale, name)
			}
		}
	}
	return j.store.RemoveMeta(stale...)
}

// what the journal found of an index that was persisted
type loaded struct {
	// lastFlush is when the index was last persisted, and zero when it never was
	lastFlush int64
	// whole is true when nothing of what was persisted is missing or damaged
	whole bool
	// clean is true when the index was closed in good order, at the generation in evidence
	clean bool
}

// returns the time of the last mark among the records
func (j *journal) replay(name string, buf []byte, apply func(r *record)) (int64, error) {
	f, err := j.store.OpenMeta(name)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	br := bufio.NewReaderSize(f, snapshotBuffer)
	var last int64
	for {
		rec, err := readRecord(br, buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			return last, err
		}
		if rec.op == opMark {
			last = rec.lastWrite
			continue
		}
		apply(&rec)
	}
}

// readClean returns the generation the clean marker vouches for. A marker that is absent,
// unreadable or carries no generation vouches for nothing at all
func (j *journal) readClean() (uint64, bool) {
	f, err := j.store.OpenMeta(cleanMarker)
	if err != nil {
		return 0, false
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, generationLen))
	if err != nil || len(b) == 0 {
		return 0, false
	}
	generation, err := strconv.ParseUint(string(b), decimal, 64)
	return generation, err == nil
}

// the newest snapshot that can be read whole, then each journal of its generation and after,
// in order. A newer snapshot that is damaged falls back to the newest intact one; when every
// snapshot is damaged, nothing is adopted and the cache is swept anew
func (j *journal) load(apply func(r *record)) (loaded, error) {
	names, err := j.store.ListMeta()
	if err != nil {
		return loaded{}, err
	}
	var l loaded
	var snapshots []uint64
	var journals []string
	var hasClean bool
	var maxGeneration uint64
	for _, name := range names {
		if g, ok := generationOf(name, snapshotPrefix); ok {
			snapshots = append(snapshots, g)
		} else if _, ok := generationOf(name, journalPrefix); ok {
			journals = append(journals, name)
		} else if name == cleanMarker {
			hasClean = true
		}
		maxGeneration = max(maxGeneration, generationOfAny(name))
	}
	slices.Sort(snapshots)
	slices.Sort(journals)
	// what follows is written to a generation of its own, and the first flush replaces them all
	j.generation = maxGeneration + 1
	j.lossy = len(snapshots) > 0 || len(journals) > 0
	buf := make([]byte, recordOverhead+maxRecordKeyLen)
	from := uint64(0)
	if len(snapshots) > 0 {
		var intact bool
		var damage error
		for i := len(snapshots) - 1; i >= 0; i-- {
			g := snapshots[i]
			last, rerr := j.replay(generationName(snapshotPrefix, g), buf, apply)
			l.lastFlush = max(l.lastFlush, last)
			if rerr != nil {
				// a snapshot is written whole or not at all, so a damaged one vouches for nothing;
				// an older, intact one plus the journals since may still be the complete truth
				damage = rerr
				continue
			}
			from, intact = g, true
			break
		}
		if !intact {
			// journals are deltas of the snapshot lineage, and are no substitute for a base
			// that cannot be proven; the cache itself is the only thing left to reconcile from
			_ = j.store.RemoveMeta(cleanMarker)
			return loaded{whole: false}, errors.Join(errDamagedIndex, damage)
		}
	}
	l.whole = true
	for _, name := range journals {
		if g, ok := generationOf(name, journalPrefix); !ok || g < from {
			continue
		}
		last, rerr := j.replay(name, buf, apply)
		l.lastFlush = max(l.lastFlush, last)
		if rerr != nil {
			// a journal torn by a crash, or one that cannot be opened, is good up to the tear,
			// and what may be past it is found again by the sweep the incomplete load asks for
			l.whole = false
		}
	}
	var cleanGeneration uint64
	if hasClean {
		cleanGeneration, l.clean = j.readClean()
		// taking the marker back is best effort: a stale one is harmless once it can vouch
		// only for the generation it names
		_ = j.store.RemoveMeta(cleanMarker)
	}
	// the mark of a good close vouches only while nothing was written past what it closed
	if l.clean && cleanGeneration < maxGeneration {
		l.clean = false
	}
	return l, nil
}

func generationOfAny(name string) uint64 {
	for _, prefix := range [...]string{snapshotPrefix, journalPrefix} {
		if g, ok := generationOf(name, prefix); ok {
			return g
		}
	}
	return 0
}

// a journal missing records is first replaced by a snapshot, and no mark of a good close is
// left if that fails. The mark carries the generation it closes, in this order: appended
// records, a complete snapshot, the clean marker -- so a restart can tell what combination
// was fully published, and a mark left by an older close cannot vouch for a later generation
func (j *journal) close(now time.Time, each func(yield func(o *Object) bool)) error {
	compact, err := j.flush(now)
	if err != nil {
		return err
	}
	if compact {
		if err = j.compact(now, each); err != nil {
			return err
		}
	}
	w, err := j.store.CreateMeta(cleanMarker)
	if err != nil {
		return err
	}
	if _, err = w.Write(strconv.AppendUint(nil, j.generation, decimal)); err != nil {
		_ = w.Close()
		return err
	}
	return w.Close()
}
