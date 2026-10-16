#!/bin/sh
# Mirror the recorder host's sealed archive into this workstation's copy
# (docs/decisions/2026-10-16-a1-host.md). The mirror is the backup and what
# `clv ingest` reads, so it must never claim more than it holds.
#
#   ARCHIVE_MIRROR_SRC  the recorder's archive root, as rsync reads it (pi:/mnt/ssd/archive)
#   ARCHIVE_MIRROR_DST  the root of this mirror (e.g. ~/code/clv-harness/archive)
#
# The seal watermark (sealed_through.json, raw_recorder.archive) says every frame
# received before it is in a sealed, journalled segment. The mirror copies a
# snapshot of it first, then the sealed segments and journals, and publishes the
# snapshot only after that: a published watermark only vouches for segments
# sealed before it was read, and those are all copied by then. A failed run
# publishes nothing, so the mirror's watermark stays at its last complete run.
set -eu
src=${ARCHIVE_MIRROR_SRC:?the recorder archive root, e.g. pi:/mnt/ssd/archive}
dst=${ARCHIVE_MIRROR_DST:?the root of this mirror}
mkdir -p "$dst"
snapshot="$dst/.sealed_through.json.mirror"
rsync -t "$src/sealed_through.json" "$snapshot"
rsync -rt \
    --exclude '*.part' --exclude 'sealed_through.json' --exclude '.sealed_through.json.*' \
    --exclude '.recorder.lock' --exclude '*.tmp' \
    "$src/" "$dst/"
mv "$snapshot" "$dst/sealed_through.json"
