# Union delta ledger

This fork's `union` branch is a **consumption branch**: it merges the
contribution branches below and is what Union tags releases from (the
binary bundled into `flyteplugins-union` wheels). Upstream never sees
`union`.

Branch layout:

- `main` — tracks juicedata/juicefs main. Sync freely; never commit here.
- `union` — merge-only, plus union-specific housekeeping commits (this
  file, `go.mod` release pins). Default branch of the fork.
- Contribution branches (below) — based on upstream lines, pristine.
  Fix bugs THERE first, then merge forward into `union` (upstream-first).
  Never rebase them onto `union` or push union-only commits to them:
  `haytham/fuse-passthrough` is the live head of upstream PR
  juicedata/juicefs#7202 — anything pushed there appears in the upstream
  PR immediately.

## Deltas vs. juicedata main

| Delta | Branch | Upstream status |
|---|---|---|
| Write-path FUSE passthrough (staging + reconcile + durability fences + registration pooling) | `haytham/fuse-passthrough` | **juicedata/juicefs#7202** (open, review promised post-1.4; depends on juicedata/go-fuse#53) |
| grpc/stats/opentelemetry ambiguous-import build fix | `haytham/fix-ambiguous-grpc-otel` | trivially upstreamable — offer any time |
| `checkpoint` control verb (durable live metadata snapshot) | `haytham/checkpoint-verb` | candidate — generically useful, pitch after #7202 |
| `checkpoint-restore` (materialize a store from a checkpoint artifact) | `haytham/checkpoint-restore` | candidate, pairs with the verb |
| `--slice-domain` (partition slice IDs per writer session) | `haytham/domain-scoped-slices` | union-only — encodes Union's volume fork/branch model |
| `slice-refs` (GC reference extraction from commit indexes) | `haytham/slice-refs` | union-only — serves Union's GC reaper design |
| `go.mod`: replace go-fuse with `github.com/unionai/go-fuse/v2` release tags | `union` only | n/a (fork plumbing; upstream PR #7202 carries its own replace pointing at go-fuse#53) |

## Releases

Tags `v1.4.0-union.N` are cut from `union`, with `linux-amd64` binary
assets built as:

```
CGO_ENABLED=1 go build -tags <see release notes; badger INCLUDED from union.1> -o juicefs .
```

`flyteplugins-union`'s `maint_tools/bundle_juicefs.py` downloads these
assets by tag; keep its `DEFAULT_VERSION` in sync when cutting a release.

## Syncing from upstream

Merge the juicedata release tag into `union` (never rebase), resolve,
then run the full harness (`/root/src/volume-v2-harness/harness.sh all`)
before tagging. As upstream PRs land, drop the corresponding branch from
the merge set — the fork should shrink toward zero delta.
