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

package vfs

import (
	"sync"
	"testing"
	"time"
)

func newTestVFSForExtFlush() *VFS {
	v := &VFS{}
	v.extFlushCond = sync.NewCond(&v.extFlushMu)
	return v
}

// TestQuiesceExternalFlushesBlocksNewAdmissions is a regression test for a
// TOCTOU in the checkpoint quiesce path: a plain "poll until
// ExternalFlushes()==0" lets a new BeginExternalFlush start in the gap
// between the poll succeeding and the caller actually taking its snapshot,
// silently including or racing data whose close(2) already returned.
// QuiesceExternalFlushes must close that gap by refusing new admissions
// before it starts waiting, so once it returns true the count cannot rise
// again until EndQuiesceExternalFlushes is called.
func TestQuiesceExternalFlushesBlocksNewAdmissions(t *testing.T) {
	v := newTestVFSForExtFlush()

	if !v.QuiesceExternalFlushes(time.Now().Add(time.Second)) {
		t.Fatalf("quiesce with nothing in flight should succeed immediately")
	}

	admitted := make(chan struct{})
	go func() {
		v.BeginExternalFlush() // must block until EndQuiesceExternalFlushes
		close(admitted)
		v.EndExternalFlush()
	}()

	select {
	case <-admitted:
		t.Fatalf("BeginExternalFlush was admitted while quiescing")
	case <-time.After(50 * time.Millisecond):
		// expected: still blocked
	}

	if got := v.ExternalFlushes(); got != 0 {
		t.Fatalf("ExternalFlushes() = %d while quiescing, want 0", got)
	}

	v.EndQuiesceExternalFlushes()

	select {
	case <-admitted:
	case <-time.After(time.Second):
		t.Fatalf("BeginExternalFlush never unblocked after EndQuiesceExternalFlushes")
	}
}

// TestQuiesceExternalFlushesWaitsForInFlight: an already in-flight flush must
// finish (EndExternalFlush) before QuiesceExternalFlushes returns true, and a
// new one started concurrently must not let the count re-observe zero early.
func TestQuiesceExternalFlushesWaitsForInFlight(t *testing.T) {
	v := newTestVFSForExtFlush()
	v.BeginExternalFlush()

	done := make(chan bool, 1)
	go func() { done <- v.QuiesceExternalFlushes(time.Now().Add(2 * time.Second)) }()

	select {
	case <-done:
		t.Fatalf("quiesce returned before the in-flight flush ended")
	case <-time.After(50 * time.Millisecond):
	}

	v.EndExternalFlush()

	select {
	case ok := <-done:
		if !ok {
			t.Fatalf("quiesce timed out after the in-flight flush ended")
		}
	case <-time.After(time.Second):
		t.Fatalf("quiesce never returned after the in-flight flush ended")
	}
	v.EndQuiesceExternalFlushes()
}

// TestQuiesceExternalFlushesTimeoutReopensGate: on timeout,
// QuiesceExternalFlushes must reopen the gate itself so callers blocked in
// BeginExternalFlush aren't stuck forever just because the checkpoint gave up.
func TestQuiesceExternalFlushesTimeoutReopensGate(t *testing.T) {
	v := newTestVFSForExtFlush()
	v.BeginExternalFlush() // never ended: forces a timeout

	if v.QuiesceExternalFlushes(time.Now().Add(20 * time.Millisecond)) {
		t.Fatalf("quiesce should time out with a permanently in-flight flush")
	}

	admitted := make(chan struct{})
	go func() {
		v.BeginExternalFlush()
		close(admitted)
	}()
	select {
	case <-admitted:
	case <-time.After(time.Second):
		t.Fatalf("gate stayed closed after QuiesceExternalFlushes timed out")
	}
}
