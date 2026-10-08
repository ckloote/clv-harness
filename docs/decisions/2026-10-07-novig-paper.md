# Novig key access, Paper testing and key provisioning

Date: 2026-10-07

Status: accepted for R0

## Finding

Novig's key docs say a management key is created at **Profile → Settings → Novig API**. Our Production account has no such entry. The Paper web app, whose bundle is public and appears to share Production's codebase, shows the entry only when `IS_DEMO || featureFlag("novig-api")`:
- `IS_DEMO` is set in the Paper build, so the entry is always present there.
- On Production the entry depends on a per-account PostHog flag that Novig controls.

The API also defines `NOVIG_API_DISABLED` (403, "Novig API not enabled"). The only documented route to Production access is LP onboarding: email developers@novig.com, receive QA access in about two business days, and make a minimum deposit of about $30,000. Read-only data access is not addressed. The docs' examples default to the Paper host, which is why they read as self-serve.

This is logged in `docs/vendor-capabilities.md`. Novig is being asked to enable read-only access. If it refuses, or wants the LP deposit for read-only use, DESIGN.md §3.2 applies: evaluate Kalshi as the primary reference.

## Decision: verify the protocol on Paper meanwhile

Paper runs the same API and lists the same MLB postseason games as Production, under different market IDs, with live play-money books. It cannot supply reference prices. It can verify our signer and stream code against the real server, and answer most of R0's wire-protocol questions:
- handshake, subscribe and acknowledgement shapes, and whether probe replies echo the nonce
- Ping cadence
- per-market, per-channel sequencing
- `book` and `trades` on one connection
- lifecycle values around first pitch, assuming Paper mirrors the real event feed
- throttle behavior and venue-to-receive lag

New Paper subaccounts start with a zero balance, so Paper also tests streaming from an unfunded subaccount, assuming its rules match Production's. Location checks cannot be tested on Paper.

Paper evidence never reaches V0 or any calibration:
- `tools/raw_recorder/config.paper.toml` writes to `archive-paper/` (git-ignored, separate lock) and records only Novig. Its `[r0]` and `[stream]` values must equal Production's, and a test enforces this.
- Paper uses its own environment variables (`NOVIG_PAPER_READ_KEY_*`) and its own game list (`games.paper.toml`).
- Any answer recorded from Paper in `vendor-capabilities.md` is labeled Paper, and is re-verified on Production when access arrives.

Per-source `enabled` flags and a config-level `games_file` make the Paper configuration a single `--config` switch. Production and Paper recorders can run side by side.

## Key provisioning tool

Getting from a management key to a `trading::read` key takes two management-signed calls, so the repo now has `novig-provision`. The recorder must never load a management key (DESIGN.md §3.2), so the tool:
- Lives in its own module. A test checks that the recorder's modules never import it.
- Opens a subaccount using a trading keypair generated in memory. The private half is never written anywhere, so no credential that can trade exists afterwards. While that trading key is live, Novig mints only `trading::read` keys for the subaccount.
- Writes the new read key with 0600 permissions, never overwriting an existing file, and before the issuing call, removing it if the call fails.
- Checks the new key: its echo must succeed, and reading key metadata must be refused.
- Asks for confirmation on Production. On Production it must run on a trusted machine, not the recorder host. On Paper, where the money is play money, it may run anywhere.

## Validation

126 tests pass, including the provisioning flow against a fake Novig that verifies every NOVIG-V3 signature and a test that a refusal removes the key file. A live 20 s run of the Paper configuration against Paper's public endpoints sealed and verified into `archive-paper/` only.
