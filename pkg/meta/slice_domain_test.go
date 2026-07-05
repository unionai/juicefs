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

import (
	"os"
	"syscall"
	"testing"
)

// Slice IDs feed directly into object keys, so two writers sharing a bucket
// under independent metadata DBs must never allocate the same ID. With
// Config.SliceDomain set, every ID carries the domain in its high bits.
func TestSliceDomain(t *testing.T) {
	_ = os.Remove(settingPath)
	conf := testConfig()
	conf.SliceDomain = 7
	m, err := newKVMeta("memkv", "jfs-slice-domain-test", conf)
	if err != nil {
		t.Fatalf("create meta: %s", err)
	}
	if err := m.Reset(); err != nil {
		t.Fatalf("reset meta: %s", err)
	}
	if err := m.Init(testFormat(), true); err != nil {
		t.Fatalf("init meta: %s", err)
	}
	ctx := Background()

	var prevRaw uint64
	for i := 0; i < 3; i++ {
		var id uint64
		if st := m.NewSlice(ctx, &id); st != 0 {
			t.Fatalf("NewSlice: %s", st)
		}
		if got := id >> sliceDomainShift; got != 7 {
			t.Fatalf("slice id %d carries domain %d, want 7", id, got)
		}
		raw := id & (1<<sliceDomainShift - 1)
		if raw == 0 {
			t.Fatalf("raw lane id must not be 0 (id %d)", id)
		}
		if i > 0 && raw <= prevRaw {
			t.Fatalf("raw lane ids not increasing: %d then %d", prevRaw, raw)
		}
		prevRaw = raw
	}

	// The composed ID must stay within 62 bits for the max domain (int64
	// allocators and the dump format cap the top).
	maxID := uint64(1<<sliceDomainBits-1)<<sliceDomainShift | (1<<sliceDomainShift - 1)
	if maxID >= 1<<63 {
		t.Fatalf("max composed slice id %d does not fit int64", maxID)
	}
}

// A session that exhausts its 42-bit lane must fail hard (ENOSPC), never
// walk into another domain's key space.
func TestSliceDomainLaneExhaustion(t *testing.T) {
	_ = os.Remove(settingPath)
	conf := testConfig()
	conf.SliceDomain = 3
	m, err := newKVMeta("memkv", "jfs-slice-domain-exhaust-test", conf)
	if err != nil {
		t.Fatalf("create meta: %s", err)
	}
	if err := m.Reset(); err != nil {
		t.Fatalf("reset meta: %s", err)
	}
	if err := m.Init(testFormat(), true); err != nil {
		t.Fatalf("init meta: %s", err)
	}
	ctx := Background()

	// Push the raw allocator to the lane boundary; the next allocation
	// must refuse rather than overflow into domain 4's space.
	base := m.getBase()
	base.freeMu.Lock()
	base.freeSlices.next = 1 << sliceDomainShift
	base.freeSlices.maxid = 1<<sliceDomainShift + sliceIdBatch
	base.freeMu.Unlock()

	var id uint64
	if st := m.NewSlice(ctx, &id); st != syscall.ENOSPC {
		t.Fatalf("NewSlice beyond the lane: got %s (id %d), want ENOSPC", st, id)
	}
}

// Domain 0 must preserve legacy behavior bit-for-bit: raw counter IDs, no
// composition, no lane limit.
func TestSliceDomainZeroIsLegacy(t *testing.T) {
	_ = os.Remove(settingPath)
	m, err := newKVMeta("memkv", "jfs-slice-domain-legacy-test", testConfig())
	if err != nil {
		t.Fatalf("create meta: %s", err)
	}
	if err := m.Reset(); err != nil {
		t.Fatalf("reset meta: %s", err)
	}
	if err := m.Init(testFormat(), true); err != nil {
		t.Fatalf("init meta: %s", err)
	}
	ctx := Background()

	var id uint64
	if st := m.NewSlice(ctx, &id); st != 0 {
		t.Fatalf("NewSlice: %s", st)
	}
	if id >> sliceDomainShift != 0 {
		t.Fatalf("legacy slice id %d has domain bits set", id)
	}

	// Beyond-lane raw IDs stay legal in the legacy space.
	base := m.getBase()
	base.freeMu.Lock()
	base.freeSlices.next = 1<<sliceDomainShift + 5
	base.freeSlices.maxid = 1<<sliceDomainShift + sliceIdBatch
	base.freeMu.Unlock()
	if st := m.NewSlice(ctx, &id); st != 0 {
		t.Fatalf("legacy NewSlice beyond 2^42: %s", st)
	}
	if id != 1<<sliceDomainShift+5 {
		t.Fatalf("legacy id passthrough broken: got %d", id)
	}
}
