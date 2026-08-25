//go:build linux

/*
 * JuiceFS, Copyright 2026 Juicedata, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License").
 */

package fuse

import (
	"math"
	"testing"
	"time"
)

// Passthrough writes land in a real file on the staging filesystem, so an open
// granted without room for the data either fails mid-write with ENOSPC or —
// when staging is a memory-backed emptyDir — pushes the pod's memory cgroup
// into reclaim, degrading every later staging write and reconcile. Declining
// the open and taking the daemon path is strictly better, so the floor must
// actually gate.
func TestStagingHeadroomGatesOnFreeSpace(t *testing.T) {
	p := &passthroughState{dir: t.TempDir(), minFree: 1} // 1 byte: any real fs clears this
	if !p.hasStagingHeadroom() {
		t.Fatalf("expected headroom with a 1-byte floor on %s", p.dir)
	}

	// An absurd floor no filesystem can satisfy must refuse.
	p2 := &passthroughState{dir: t.TempDir(), minFree: math.MaxUint64}
	if p2.hasStagingHeadroom() {
		t.Fatalf("expected no headroom with a MaxUint64 floor")
	}
}

// minFree == 0 disables the check outright, so operators can opt out without
// having to guess a number that always passes.
func TestStagingHeadroomDisabledWhenZero(t *testing.T) {
	p := &passthroughState{dir: "/nonexistent-path-for-statfs", minFree: 0}
	if !p.hasStagingHeadroom() {
		t.Fatalf("minFree=0 must skip the check entirely, even for an unstattable dir")
	}
}

// A statfs error must read as "no headroom": we cannot confirm room, and the
// daemon path is always correct, merely slower. The failure mode we refuse to
// have is granting passthrough onto a filesystem we know nothing about.
func TestStagingHeadroomFailsClosedOnStatfsError(t *testing.T) {
	p := &passthroughState{dir: "/nonexistent-path-for-statfs", minFree: 1}
	if p.hasStagingHeadroom() {
		t.Fatalf("statfs failure must be treated as no headroom")
	}
}

// Open is a hot path — a small-file loop issues thousands per second — so the
// result is cached for stagingFreeTTL rather than re-stat'ing every open.
func TestStagingHeadroomCachesWithinTTL(t *testing.T) {
	p := &passthroughState{dir: t.TempDir(), minFree: 1}
	if !p.hasStagingHeadroom() {
		t.Fatalf("expected headroom")
	}
	// Poison the cached verdict; within the TTL the cached value must win,
	// proving we did not statfs again.
	p.mu.Lock()
	p.freeOK = false
	p.mu.Unlock()
	if p.hasStagingHeadroom() {
		t.Fatalf("expected the cached (poisoned) verdict to be reused within the TTL")
	}

	// Expire the cache: the real filesystem is consulted again and wins.
	p.mu.Lock()
	p.freeCheckAt = time.Now().Add(-2 * stagingFreeTTL)
	p.mu.Unlock()
	if !p.hasStagingHeadroom() {
		t.Fatalf("expected a fresh statfs after the TTL expired")
	}
}
