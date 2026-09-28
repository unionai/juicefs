//go:build !nobadger

/*
 * JuiceFS, Copyright 2026 Juicedata, Inc.
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

package meta

// Incremental (delta) checkpoints of a Badger store.
//
// A full checkpoint copies every key; its cost grows with the store, not with
// what changed. A delta checkpoint records only the keys that changed since a
// BASE: the latest value of each, or a tombstone for each deleted one.
// Restoring is "untar the full checkpoint the chain starts from, then apply
// each delta in order".
//
// The base is the state of the last checkpoint the caller PUBLISHED from this
// client -- not merely the last one written, since a checkpoint can be
// written and then fail to publish. So every checkpoint (full or delta) is
// held as PENDING, and the caller promotes one to base with
// ConfirmCheckpoint once it is published. A delta is always taken against the
// confirmed base, so after an unpublished checkpoint the next delta simply
// covers more (it is cumulative from the base), and stays correct.
//
// The base is held as an open read transaction at its timestamp. That is
// what makes the delta complete: Badger's compaction may drop a tombstone
// (and the versions under it) once no reader can see below it, and a dropped
// tombstone newer than the base is a deletion the delta would silently miss.
// A held reader keeps every version newer than the base alive. The cost is
// that nothing newer than the base is garbage-collected until the next
// confirm, so a hold older than deltaHoldMaxAge is released: the next delta
// request then fails with ErrNoDeltaBase and the caller takes a full
// checkpoint instead.
//
// The base at open is the store as it was opened: for a store just restored
// from a chain, exactly the published version it was restored from.

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/juicedata/juicefs/pkg/utils"
)

// ErrNoDeltaBase: there is no confirmed checkpoint to take a delta against.
var ErrNoDeltaBase = errors.New("no delta base: this client holds no confirmed checkpoint to take a delta against (take a full checkpoint)")

// deltaHoldMaxAge bounds how long a base (and so every version newer than
// it) is kept alive without a confirm.
var deltaHoldMaxAge = 24 * time.Hour

// maxPendingCheckpoints bounds the unconfirmed checkpoints held at once.
const maxPendingCheckpoints = 4

const deltaMagic = "JFSBDLT1"

const (
	deltaOpEnd byte = 0
	deltaOpSet byte = 1
	deltaOpDel byte = 2
)

var deltaCRC = crc32.MakeTable(crc32.Castagnoli)

type deltaHolds struct {
	mu      sync.Mutex
	base    *badger.Txn
	baseAt  time.Time
	pending []*badger.Txn // ascending ReadTs
}

func (h *deltaHolds) init(db *badger.DB) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.base = db.NewTransaction(false)
	h.baseAt = time.Now()
}

// addPending takes ownership of txn, a checkpoint's read transaction.
func (h *deltaHolds) addPending(txn *badger.Txn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.pending = append(h.pending, txn)
	sort.Slice(h.pending, func(i, j int) bool { return h.pending[i].ReadTs() < h.pending[j].ReadTs() })
	for len(h.pending) > maxPendingCheckpoints {
		h.pending[0].Discard()
		h.pending = h.pending[1:]
	}
}

// confirm promotes the pending checkpoint at ts to base and drops every hold
// older than it.
func (h *deltaHolds) confirm(ts uint64) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	idx := -1
	for i, t := range h.pending {
		if t.ReadTs() == ts {
			idx = i
			break
		}
	}
	if idx < 0 {
		return fmt.Errorf("no pending checkpoint at %d", ts)
	}
	if h.base != nil {
		h.base.Discard()
	}
	h.base, h.baseAt = h.pending[idx], time.Now()
	for _, t := range h.pending[:idx] {
		t.Discard()
	}
	h.pending = append([]*badger.Txn(nil), h.pending[idx+1:]...)
	return nil
}

// expire releases a base held longer than maxAge. Returns whether it did.
func (h *deltaHolds) expire(maxAge time.Duration) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.base == nil || time.Since(h.baseAt) <= maxAge {
		return false
	}
	h.base.Discard()
	h.base = nil
	return true
}

// drop releases the pending checkpoint at ts (it was written but is unusable).
func (h *deltaHolds) drop(ts uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i, t := range h.pending {
		if t.ReadTs() == ts {
			t.Discard()
			h.pending = append(h.pending[:i:i], h.pending[i+1:]...)
			return
		}
	}
}

func (h *deltaHolds) close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.base != nil {
		h.base.Discard()
		h.base = nil
	}
	for _, t := range h.pending {
		t.Discard()
	}
	h.pending = nil
}

func (c *badgerClient) confirmCheckpoint(ts uint64) error { return c.holds.confirm(ts) }

// checkpointDeltaTo writes a delta of this store against the confirmed base
// to dst, read through one transaction (called back through pinned as soon as
// it exists), and holds that transaction as pending.
func (c *badgerClient) checkpointDeltaTo(dst string, pinned func()) (base, readTs uint64, err error) {
	c.holds.mu.Lock()
	// Held for the whole write: the base must not be released (and the
	// versions above it compacted away) while the delta reads them.
	defer c.holds.mu.Unlock()
	if c.holds.base == nil {
		return 0, 0, ErrNoDeltaBase
	}
	base = c.holds.base.ReadTs()
	txn := c.client.NewTransaction(false)
	if pinned != nil {
		pinned()
	}
	readTs = txn.ReadTs()
	if err = writeDelta(txn, base, dst); err != nil {
		txn.Discard()
		return 0, 0, err
	}
	// addPending takes the lock itself; we already hold it.
	c.holds.pending = append(c.holds.pending, txn)
	for len(c.holds.pending) > maxPendingCheckpoints {
		c.holds.pending[0].Discard()
		c.holds.pending = c.holds.pending[1:]
	}
	return base, readTs, nil
}

func writeDelta(txn *badger.Txn, base uint64, dst string) (err error) {
	tmp, err := os.CreateTemp(filepath.Dir(dst), ".badger-delta-")
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmp.Name())
		}
	}()
	crc := crc32.New(deltaCRC)
	bw := bufio.NewWriterSize(io.MultiWriter(tmp, crc), 1<<20)
	var hdr [8 + 8 + 8]byte
	copy(hdr[:8], deltaMagic)
	binary.BigEndian.PutUint64(hdr[8:], base)
	binary.BigEndian.PutUint64(hdr[16:], txn.ReadTs())
	if _, err = bw.Write(hdr[:]); err != nil {
		return err
	}
	it := txn.NewIterator(badger.IteratorOptions{AllVersions: true, SinceTs: base, PrefetchValues: true, PrefetchSize: 256})
	defer it.Close()
	var last []byte
	var count uint64
	var vbuf [binary.MaxVarintLen64]byte
	putBytes := func(b []byte) error {
		n := binary.PutUvarint(vbuf[:], uint64(len(b)))
		if _, err := bw.Write(vbuf[:n]); err != nil {
			return err
		}
		_, err := bw.Write(b)
		return err
	}
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		if last != nil && string(item.Key()) == string(last) {
			continue // an older version of a key already recorded
		}
		last = item.KeyCopy(last[:0])
		if item.IsDeletedOrExpired() {
			if err = bw.WriteByte(deltaOpDel); err != nil {
				return err
			}
			if err = putBytes(last); err != nil {
				return err
			}
		} else {
			v, verr := item.ValueCopy(nil)
			if verr != nil {
				return verr
			}
			if err = bw.WriteByte(deltaOpSet); err != nil {
				return err
			}
			if err = putBytes(last); err != nil {
				return err
			}
			if err = bw.WriteByte(item.UserMeta()); err != nil {
				return err
			}
			if err = putBytes(v); err != nil {
				return err
			}
		}
		count++
	}
	if err = bw.WriteByte(deltaOpEnd); err != nil {
		return err
	}
	var cnt [8]byte
	binary.BigEndian.PutUint64(cnt[:], count)
	if _, err = bw.Write(cnt[:]); err != nil {
		return err
	}
	if err = bw.Flush(); err != nil {
		return err
	}
	var sum [4]byte
	binary.BigEndian.PutUint32(sum[:], crc.Sum32())
	if _, err = tmp.Write(sum[:]); err != nil {
		return err
	}
	if err = tmp.Sync(); err != nil {
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), dst)
}

// DeltaInfo describes a delta checkpoint file.
type DeltaInfo struct {
	Base, ReadTs, Records uint64
}

// readDelta parses and verifies a delta file; apply, if set, receives every
// record in order. With apply nil it only verifies.
func readDelta(path string, apply func(op byte, key []byte, userMeta byte, val []byte) error) (DeltaInfo, error) {
	var info DeltaInfo
	f, err := os.Open(path)
	if err != nil {
		return info, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return info, err
	}
	if st.Size() < 8+8+8+1+8+4 {
		return info, fmt.Errorf("%s: too short to be a delta checkpoint", path)
	}
	body := io.LimitReader(f, st.Size()-4)
	crc := crc32.New(deltaCRC)
	br := bufio.NewReaderSize(io.TeeReader(body, crc), 1<<20)
	var hdr [24]byte
	if _, err = io.ReadFull(br, hdr[:]); err != nil {
		return info, err
	}
	if string(hdr[:8]) != deltaMagic {
		return info, fmt.Errorf("%s: not a delta checkpoint", path)
	}
	info.Base = binary.BigEndian.Uint64(hdr[8:])
	info.ReadTs = binary.BigEndian.Uint64(hdr[16:])
	readBytes := func() ([]byte, error) {
		n, err := binary.ReadUvarint(br)
		if err != nil {
			return nil, err
		}
		if n > uint64(st.Size()) {
			return nil, fmt.Errorf("%s: corrupt record length %d", path, n)
		}
		b := make([]byte, n)
		_, err = io.ReadFull(br, b)
		return b, err
	}
	for {
		op, err := br.ReadByte()
		if err != nil {
			return info, fmt.Errorf("%s: truncated: %w", path, err)
		}
		if op == deltaOpEnd {
			break
		}
		key, err := readBytes()
		if err != nil {
			return info, fmt.Errorf("%s: record %d: %w", path, info.Records, err)
		}
		var um byte
		var val []byte
		switch op {
		case deltaOpSet:
			if um, err = br.ReadByte(); err != nil {
				return info, fmt.Errorf("%s: record %d: %w", path, info.Records, err)
			}
			if val, err = readBytes(); err != nil {
				return info, fmt.Errorf("%s: record %d: %w", path, info.Records, err)
			}
		case deltaOpDel:
		default:
			return info, fmt.Errorf("%s: record %d: unknown op %d", path, info.Records, op)
		}
		if apply != nil {
			if err = apply(op, key, um, val); err != nil {
				return info, err
			}
		}
		info.Records++
	}
	var cnt [8]byte
	if _, err = io.ReadFull(br, cnt[:]); err != nil {
		return info, fmt.Errorf("%s: truncated trailer: %w", path, err)
	}
	if n := binary.BigEndian.Uint64(cnt[:]); n != info.Records {
		return info, fmt.Errorf("%s: record count %d, trailer says %d", path, info.Records, n)
	}
	if _, err = br.ReadByte(); err != io.EOF {
		return info, fmt.Errorf("%s: trailing bytes after the record count", path)
	}
	var sum [4]byte
	if _, err = io.ReadFull(f, sum[:]); err != nil {
		return info, err
	}
	if got, want := crc.Sum32(), binary.BigEndian.Uint32(sum[:]); got != want {
		return info, fmt.Errorf("%s: checksum mismatch (%08x != %08x)", path, got, want)
	}
	return info, nil
}

// IsStoreDelta reports whether path holds a delta checkpoint.
func IsStoreDelta(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	var m [8]byte
	if _, err := io.ReadFull(f, m[:]); err != nil {
		return false
	}
	return string(m[:]) == deltaMagic
}

// ApplyStoreDeltas applies delta checkpoints, in order, to the Badger store
// in dir -- normally one just restored from the full checkpoint the chain
// starts from. Every delta is verified before any is applied, so a corrupt
// or truncated one leaves the store untouched.
func ApplyStoreDeltas(dir string, deltas []string) error {
	for _, d := range deltas {
		if _, err := readDelta(d, nil); err != nil {
			return err
		}
	}
	opt := badger.DefaultOptions(dir)
	opt.Logger = utils.GetLogger("badger")
	opt.MetricsEnabled = false
	db, err := badger.Open(opt)
	if err != nil {
		return err
	}
	defer db.Close()
	for _, d := range deltas {
		wb := db.NewWriteBatch()
		_, err := readDelta(d, func(op byte, key []byte, um byte, val []byte) error {
			if op == deltaOpDel {
				return wb.Delete(key)
			}
			return wb.SetEntry(badger.NewEntry(key, val).WithMeta(um))
		})
		if err != nil {
			wb.Cancel()
			return err
		}
		if err = wb.Flush(); err != nil {
			return err
		}
	}
	return db.Sync()
}
