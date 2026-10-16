# Measurement Contract — NBA Moneyline

**Status:** draft. It must be signed off before A2 links Novig markets to NBA events (`docs/decisions/2026-10-16-a1-league.md`). Not frozen.  
**Date:** 2026-10-16

This is the NBA version of §2–§3 of `docs/measurement-contract.md`, as that document's §1 requires. The vocabulary, instruments and polarity (§4), primary analysis population (§5) and change rules come from that document unchanged, read with "game" for an NBA game and "tip-off" for first pitch. Thresholds stay in DESIGN.md §10.

Every rule below is marked as either **verified** or **unverified**, and the owning phase verifies the unverified ones. Nothing here is primary-eligible yet.

## 1. Scope

| In | Not in |
|---|---|
| Full-game moneyline: two outcomes, "team X wins this game", overtime included | Spreads, totals, quarter and half markets, props |
| Regular-season, Play-In and playoff games | Preseason, the All-Star Game and its events |
| Pregame entries | In-play entries |

## 2. The canonical contract

**Canonical event:** one NBA game, identified by the league's `gameId`.
- Its first digits are believed to encode the season type: preseason, regular season, All-Star, playoffs, Play-In. **Unverified — A2.**
- The date and teams alone never identify a game.
- Whether a postponed game keeps its `gameId` is **unverified — A2**.

**Canonical outcome:** `P(team X wins game gameId)`, with the winner taken from the league's official final result. The official source and its fields are chosen in A2. The two outcomes are exact complements: an NBA game plays overtime periods until there is a winner.

**Settlement states.** One per event, assigned after the game from the league's status history:

| State | What happened | Novig `MONEY` |
|---|---|---|
| **N1** | Tipped off and completed, including overtime | Winner (assumed; unverified) |
| **N2** | Postponed before tip-off, played later | Unverified. The MLB analogue (S5) settled on the rescheduled game; for NBA, `voids: FMV` may instead settle at a fair value |
| **N3** | Postponed and cancelled | `FMV` (assumed from `voids`) |
| **N4** | Started and not completed | Unverified; expected to be extremely rare |

Sources: the `voids: FMV` catalog field is observed on every NBA market (`docs/decisions/2026-10-16-a1-league.md`). Novig's NBA rules text is **unverified — A2**.

## 3. Settlement equivalence

The verdicts and their meaning are those of `docs/measurement-contract.md` §3, with N1 in place of S1–S2 and N2–N4 in place of S3–S6. As there:
- a postponement (N2, N3) is known before the close, so it never has a trusted off for its scheduled date;
- an incomplete game (N4) is decided after the off, independently of the entry's side.

**Current verdicts:** every pair is `pending` until Novig's NBA rules, and each entry venue's NBA house rules, are read.

## 4. Open items

1. **Novig's NBA rules text:** what happens on postponement, cancellation and abandonment (A2).
2. **The official NBA source:** the `gameId` meaning, schedule revisions, final results and the tip-off time. Also its terms for personal research use (A2). The A1 collector archives the source's raw responses from day one, so A2 can parse them later.
3. **Entry venues' NBA house rules** (A4).
