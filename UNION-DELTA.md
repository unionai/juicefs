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

Tags `v1.4.0-union.N` are cut from `union`. Pushing such a tag runs
`.github/workflows/union-release.yml`, which builds **linux-amd64 and
linux-arm64** on native runners and attaches
`juicefs-<version>-<platform>.tar.gz` to the release. (`workflow_dispatch`
rebuilds an existing tag.) The build is:

```
CGO_ENABLED=1 go build -tags nogateway,nowebdav,nocos,nobos,nohdfs,noibmcos,noobs,nooss,noqingstor,nosftp,noswift,noufile,nob2,nonfs,nodragonfly,nomysql,nopg,notikv,noetcd,nocifs,nostorj,noqiniu,notos,noks3
```

Backends kept: s3, gs, wasb, sqlite, redis, badger. `nobadger` is
deliberately absent and `nosqlite` too — badger is `flyteplugins-union`'s
default metadata store, and sqlite is why the build needs CGO.

arm64 was added in union.3. Before it, `union.1`/`union.2` shipped
linux-amd64 only, built by hand; `bundle_juicefs.py` falls back to upstream
juicefs for any platform with no fork asset, so an arm64 wheel silently
carried a client with no badger, no passthrough and no broker delegation —
harmless until badger became the default store type, at which point an
arm64 node would create volumes it could not itself mount.

`flyteplugins-union`'s `maint_tools/bundle_juicefs.py` downloads these
assets by tag; keep its `DEFAULT_VERSION` (and `_backend.py`'s
`_CLIENT_VERSION`) in sync when cutting a release.

## Syncing from upstream

Merge the juicedata release tag into `union` (never rebase), resolve,
then run the full harness (`/root/src/volume-v2-harness/harness.sh all`)
before tagging. As upstream PRs land, drop the corresponding branch from
the merge set — the fork should shrink toward zero delta.
