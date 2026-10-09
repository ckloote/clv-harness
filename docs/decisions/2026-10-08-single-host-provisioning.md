# Novig key provisioning on a single host

Date: 2026-10-08

Status: accepted for R0; revisit when the A1 host is chosen

## Context

DESIGN.md §13 required the Production management private key never to touch the recorder host: provision on a separate trusted machine, then copy only the read key across. The operator has one machine, and R0 runs on it (`2026-10-06-r0-recorder.md`, "Host and windows"). The rule cannot be followed. The 2026-10-08 Production provisioning ran on the recorder host.

## What the rule protected

The management key opens and funds subaccounts and creates `trading` keys. Anyone holding it can therefore trade the account's balance. A `trading::read` key alone cannot. The risk is a compromise of the recorder host while the management key is on it.

## Decision

The management private key may be on the recorder host **only for a provisioning session**:

1. **During provisioning,** keep the management PEM at 0600 outside the repo, the archive and `~/.config/raw-recorder/`.
2. **After provisioning,** once `raw-recorder echo` succeeds with the new read key, remove the management PEM from the host. Keep it offline only if you want to re-provision without creating a new key, for example on removable media or in a password manager. Otherwise create a new management key in the web app when one is next needed. Whether a new management key replaces the old one (one live key per trader) is unverified.
3. **The recorder still never loads a management key.** Separate environment variables, the test that the recorder never imports `provision`, and the startup scope check that refuses a key able to read `/v3/keys` all stay.
4. **`novig-provision` reminds you** on Production to remove the management key once the read key works.
5. **Residual risk:** while the management key is on disk, a host compromise exposes the account balance. Read-only use needs no balance (vendor statement, 2026-10-08), so keep the Production balance minimal for as long as the key is present.

The Paper management key is play money and may stay on the host.

## Unchanged

DESIGN.md §3.2: the harness and recorder load only a `trading::read` credential. If a later phase moves the recorder to its own host, such as the A1 host, the management key never goes on that host. Provisioning then happens here and only the read key is copied across.
