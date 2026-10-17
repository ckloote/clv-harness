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
# snapshot of it first, then the day journals, then the sealed segments, and
# installs the journals and publishes the snapshot only after that: a published
# watermark only vouches for segments sealed before it was read, and a journal
# only lists segments sealed before it was read, and those are all copied by
# then. A failed run publishes nothing, so the mirror's watermark stays at its
# last complete run.
#
# A day journal (manifest.jsonl) is append-only, and rsync replaces whole files.
# So the mirror never replaces a journal with one that does not extend it: that
# would mean a second writer on the same archive day, and copying it would drop
# the mirror's entries for segments it holds. The run then stops before copying
# anything. Only the copy's complete lines must match, since a final line copied
# mid-append may since have been completed, or removed as a torn tail.
set -eu
src=${ARCHIVE_MIRROR_SRC:?the recorder archive root, e.g. pi:/mnt/ssd/archive}
dst=${ARCHIVE_MIRROR_DST:?the root of this mirror}
mkdir -p "$dst"
snapshot="$dst/.sealed_through.json.mirror"
stage=$(mktemp -d "${TMPDIR:-/tmp}/archive-mirror.XXXXXX")
trap 'rm -rf "$stage"' EXIT
rsync -t "$src/sealed_through.json" "$snapshot"
rsync -rtm --include '*/' --include 'manifest.jsonl' --include 'manifest.json' --exclude '*' \
    "$src/" "$stage/"
(cd "$stage" && find . -name manifest.jsonl) > "$stage/.journals"
while read -r rel; do
    old="$dst/$rel"
    [ -f "$old" ] || continue
    keep=$(wc -c < "$old")
    if [ "$keep" -gt 0 ] && [ -n "$(tail -c 1 "$old" | tr -d '\n')" ]; then
        keep=$((keep - $(tail -n 1 "$old" | wc -c)))
    fi
    if ! cmp -s -n "$keep" "$old" "$stage/$rel"; then
        echo "archive-mirror: $rel on the recorder host does not extend this mirror's copy;" \
             "another writer on that day? Nothing copied." >&2
        exit 1
    fi
done < "$stage/.journals"
rm "$stage/.journals"
rsync -rt \
    --exclude '*.part' --exclude 'sealed_through.json' --exclude '.sealed_through.json.*' \
    --exclude '.recorder.lock' --exclude '.recorder.control' --exclude '*.tmp' \
    --exclude 'manifest.jsonl' --exclude 'manifest.json' \
    "$src/" "$dst/"
rsync -rt "$stage/" "$dst/"
mv "$snapshot" "$dst/sealed_through.json"
