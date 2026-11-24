# WD MyCloud Gen2 ARMv7 release candidate (2026-11-24)

This branch combines the two cache fixes validated by playback tests with an
opt-in OpenSSL piece SHA-1 implementation in a **separate torrent module**.
The experimental file-edge retention policy is deliberately excluded. RAM
segment eviction counters (`evict segments`, `bytes`, and `busy`) are retained
independently to enable baseline measurements without metadata pinning.

## Exact source versions

- TorrServer cache branch parent: `fix/segment-byte-accounting`,
  `8c3abf9700aab3e0d21866d0cfb86e92332ea32e`.
  Includes stale priority cleanup after segment eviction and exact per-segment
  unique-byte accounting.
- Torrent engine SHA-1 branch: `Mr-Tao/torrent:experiment/sha1-openssl-neon`,
  `716ee20efd33abb3c935c51b6f262f5a5f0b2c0c`.
  Its SHA-1 backend is opt-in and uses `libcrypto.so.1.1` or `.3` at runtime.
- The `TorrServer/server/go.mod` replace directive resolves the torrent
  module via `../../torrent-streaming`. Check out that engine side-by-side
  with `torrserver-cachefix`. This is a local release recipe, not an
  upstream-compatible module dependency change.

## Source checkout

```sh
cd ~/src
git clone https://github.com/Mr-Tao/TorrServer.git torrserver-cachefix
git clone https://github.com/Mr-Tao/torrent.git torrent-streaming

git -C torrserver-cachefix switch release/mycloud-armv7-20261124
git -C torrent-streaming switch release/mycloud-armv7-20261124

git -C torrent-streaming rev-parse HEAD
cd torrserver-cachefix/server
go list -m -json github.com/anacrolix/torrent
```

For existing worktrees, fetch the branches and switch to them instead of
re-cloning. The expected torrent engine checkout SHA is `716ee20e`.
The `go list -m -json` output should contain a `Replace.Dir` pointing
to `~/src/torrent-streaming`.

## Local test suite

```sh
cd ~/src/torrserver-cachefix/server
TS_SHA1_OPENSSL=0 go test -tags=nosqlite ./torr/storage/torrstor -count=1 -timeout=5m
TS_SHA1_OPENSSL=0 go test -race -tags=nosqlite ./torr/storage/torrstor -count=1 -timeout=10m
```

## Cross-compile a single binary with both SHA-1 backends

```sh
cd ~/src/torrserver-cachefix

podman run --rm \
  -v "$(go env GOROOT):/opt/go:ro" \
  -v "$HOME/src:/src" \
  -v /mnt/nasik-firefox-build:/output \
  -w /src/torrserver-cachefix/server \
  -e GOROOT=/opt/go \
  -e CGO_ENABLED=1 \
  -e GOOS=linux \
  -e GOARCH=arm \
  -e GOARM=7 \
  -e CC=arm-linux-gnueabihf-gcc \
  -e PATH=/opt/go/bin:/usr/local/bin:/usr/bin:/bin \
  wd-armhf-cgo \
  go build -tags=nosqlite \
    -o /output/TorrServer-linux-armv7-cachefix-openssl \
    ./cmd
```

The executable **contains** the opt-in ARMv7 OpenSSL implementation, but does
not use it unless `TS_SHA1_OPENSSL=1` is set. No OpenSSL link-time dependency
is needed: it dynamically loads `libcrypto.so.1.1` or `libcrypto.so.3`.

## Runtime and rollback

Preserve the previously working executable for rollback. Compare both runtime
backends using this same executable, same stream, and the same 64 MiB RAM
cache. Use a fresh process for each comparison.

```sh
TS_PATCH=1 TS_STATS=1 TS_SHA1_OPENSSL=0 \
  ./TorrServer-linux-armv7-cachefix-openssl -p 8090 -d ./sha1-test-data/

# After stopping the process, optionally retest with SHA-1 acceleration:
TS_PATCH=1 TS_STATS=1 TS_SHA1_OPENSSL=1 \
  ./TorrServer-linux-armv7-cachefix-openssl -p 8090 -d ./sha1-test-data/
```

Backend selection affects **torrent piece verification only**, not all SHA-1
operations or the general download path. `TS_SHA1_OPENSSL=1` requires an
available OpenSSL libcrypto ABI on the target; the adapter intentionally
fails instead of silently falling back when explicitly enabled.

Record CSTAT cache capacity/filled and reader counts, playback/seek behavior,
CPU ticks, RSS and VmSwap, and check that idle CPU returns to baseline after
the last torrent is removed.

## Not included

The `fix/segment-file-edges` prototype pins the beginnings and ends of active
files. Its combined protection budget with reader windows is not yet proven
safe, and repeated metadata reads have not been demonstrated in the target
player. It is intentionally left out of this release candidate.
