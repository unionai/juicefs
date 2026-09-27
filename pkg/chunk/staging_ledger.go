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

package chunk

import (
	"os"
	"sync"
)

// stagingLedger records the order in which blocks entered the writeback
// staging area, so a caller can wait for exactly the blocks staged before
// some moment instead of for the staging area to empty.
//
// That is what a snapshot needs. A slice reaches the metadata engine only
// after every one of its blocks is staged (or uploaded directly), so a
// metadata snapshot taken at time T references only blocks staged before T.
// Waiting for the staging gauge to reach zero instead also waits for blocks
// written after T, which a busy writer keeps adding: the wait chases the
// writer and may never end.
type stagingLedger struct {
	mu   sync.Mutex
	seq  uint64
	keys map[string]stagedBlock
}

type stagedBlock struct {
	seq  uint64
	path string
}

var ledger = &stagingLedger{keys: make(map[string]stagedBlock)}

// add records that key was staged at path. A key already present keeps its
// original sequence: it has been staged since then.
func (l *stagingLedger) add(key, path string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seq++
	if _, ok := l.keys[key]; !ok {
		l.keys[key] = stagedBlock{l.seq, path}
	}
}

// remove records that key left staging (uploaded, or dropped).
func (l *stagingLedger) remove(key string) {
	l.mu.Lock()
	delete(l.keys, key)
	l.mu.Unlock()
}

func (l *stagingLedger) mark() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.seq
}

// pendingThrough counts the blocks staged at or before mark that are still
// staged. An entry whose staging file no longer exists is dropped rather
// than counted: every path that removes a staged block is supposed to call
// remove, but a missed one must cost a stale ledger entry, not a wait that
// can only end in a timeout.
func (l *stagingLedger) pendingThrough(mark uint64) int {
	l.mu.Lock()
	var candidates []string
	for key, b := range l.keys {
		if b.seq <= mark {
			candidates = append(candidates, key)
		}
	}
	l.mu.Unlock()
	n := 0
	for _, key := range candidates {
		l.mu.Lock()
		b, ok := l.keys[key]
		l.mu.Unlock()
		if !ok {
			continue
		}
		if b.path != "" {
			if _, err := os.Stat(b.path); os.IsNotExist(err) {
				l.mu.Lock()
				if cur, ok := l.keys[key]; ok && cur.seq == b.seq {
					delete(l.keys, key)
				}
				l.mu.Unlock()
				continue
			}
		}
		n++
	}
	return n
}

// StagingMark returns a mark covering every block staged so far. Blocks
// staged later get higher sequence numbers and are not covered.
func StagingMark() uint64 { return ledger.mark() }

// PendingStagedThrough reports how many blocks staged at or before mark
// (see StagingMark) have not yet left the staging area.
func PendingStagedThrough(mark uint64) int { return ledger.pendingThrough(mark) }
