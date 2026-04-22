# Football 3-Way Arbitrage Engine

## What arb we're doing

Football matches resolve to exactly one of three outcomes: team A wins,
draw, or team B wins. Polymarket exposes each outcome as a separate
binary `neg_risk` market ("Will X win?", "end in a draw?", "Will Y
win?"). Buying 1 YES share on each of the 3 markets costs
`sum_of_best_asks` USDC and pays exactly $1 on resolution — because one
and only one of the three questions resolves YES.

**Arb condition:** `1.0 − (ask_A + ask_Draw + ask_B) − fees > 0`.
Expressed as edge %: `(1.0 − cost) / cost`. A healthy match market has
tight spreads summing to ~0.98-1.02; arb windows open when bookmaker-
style imbalance pushes the sum under 1.0 (usually when a favorite's
ask drifts down on thin liquidity).

## Core defensive principle

**No automated panic-selling.** If a multi-leg trade ends up partial-
filled, the UI surfaces three explicit recovery buttons — Fill missing
leg(s), Unwind filled legs, Hold through resolution — and the human
picks. The machine never market-sells to "save" a position.

The FOK + depth + snapshot rails below exist to *prevent* partial fills
in the first place. Manual recovery is the safety net when something
slips through.

## Defensive properties

**Never hold accidental directional exposure:**

1. **FOK (fill-or-kill) orders only.** Each leg is all-or-nothing.
   Polymarket CLOB supports `order_type=FOK`. Either the full size
   matches immediately or the order is canceled — no resting maker
   residue, no partial fill.
2. **Depth headroom.** Require ≥ 1.5× our target size at each best ask
   before firing. Minor top-of-book movement during submission won't
   tank a leg.
3. **Book-snapshot gate at execute.** Re-fetch all 3 books within a
   small window (< 500 ms) of firing; if any leg's best-ask price moved
   up or depth fell below the requirement, abort the whole arb before
   signing any orders.
4. **Manual recovery (not auto).** If despite the above we end up
   holding < 3 legs (stale state / SDK edge case / exchange oddity),
   the arb shows as a "stuck" entry with three user-selectable recovery
   actions. **The engine does not auto-sell.**

**Never fire on a bad signal:**

5. **Edge margin gate.** Minimum 2% net edge after fees. Single-digit-
   bp edges get consumed by inter-book slippage.
6. **Market-grouping confidence.** Only fire when we're confident the 3
   binaries belong to one match. Strongest signal = shared
   Polymarket `event_id` / event slug (same parent event groups all 3
   outcome markets). Weaker fallback = title pattern (`"Will X win"` /
   `"end in a draw"` / `"Will Y win"` with consistent team names and
   shared match date). Ambiguous groupings go on a review list, don't
   fire.
7. **Event-type whitelist.** Football regular-season only to start. No
   penalty-shootout-decided markets, no "could be abandoned" cup games,
   no knockout-round-with-aggregate until each edge case is validated
   against actual Polymarket resolution rules.
8. **Per-market fee fetch at scan time.** Pull taker fee from
   `/markets/{condition_id}` before computing edge — never assume zero.

**Blast-radius caps:**

9. Reuse existing `max_trade_usd` per-arb and rolling 24h `max_daily`.
10. **Off by default + manual arm.** Just like HOOVER v2 —
    confirmation preview before arming. **Auto-disarm on first unexpected
    error class** (geoblock, insufficient balance, rate-limit): one
    failure pauses the engine until re-armed.
11. **Phased rollout.** Each phase must run boring for a day before
    the next phase arms.

## Phases

### Phase 1 — Scanner only (read-only)

**Goal:** see what opportunities exist. Zero capital at risk.

- `GET /events?tag_slug=soccer&active=true&closed=false` from Gamma.
  Filter to events that have exactly 3 binary `neg_risk` child markets
  with the title pattern (X-wins / draw / Y-wins).
- For each candidate match, fetch the top-of-book for all 3 legs via
  the existing `state.book_store.fetch_rest_book` path.
- Fetch per-market taker fee from CLOB `/markets/{cid}`.
- Compute: `sum_asks`, `fee_per_share_set`, `edge_per_share`,
  `edge_pct`, `max_fillable_shares` (min leg depth, ÷ 1.5 for
  headroom).
- Display on a new `/arb` page with live-refreshing table:
  - Match title + kick-off time
  - Per-leg: best ask, depth (shares), taker fee
  - Sum of asks, edge $/share, edge %, max fillable shares
  - "Would fire at current state: YES/NO" + reasons if NO (sub-2%
    edge / no asks / thin depth / fee lookup failed)
- **No orders. No paper trades. No activity log entries.** Pure
  observation.

### Phase 2 — Paper-fire

**Goal:** log simulated fills and track projected P&L, confirm the
signal translates to profitable trades over a day or two of paper runs.

- Add a "Paper fire" button per arb row on `/arb`.
- Click → preview modal shows exact size (N shares per leg), per-leg
  cost, total cost, fee, projected profit = `N - total_cost`.
- Confirm → write a `PAPER ARB` entry to the harvester activity log
  (`activity.jsonl`) with:
  - `strategy = "PAPER ARB"`
  - `market` = match title
  - `buy_cost` = sum of (N × ask_i) per leg
  - `sell_revenue` = N (assumed $1 payout on the one YES that resolves)
  - `net_profit` = `sell_revenue − buy_cost − fees`
  - `status = "PAPER ARB OK @ current asks"`
- Surfaces naturally on `/trades` Activity Log filter, and existing
  `compute_stats` PAPER skip keeps it out of monetary totals.
- Still no real orders.

### Phase 3 — Live execute (deferred, separate plan before shipping)

Sketched here for completeness; will land as its own PR with a fresh
pre-flight review after Phase 2 runs clean.

- `POST /api/arb/execute` handler. Receives `{event_id, size_shares}`.
- Pre-flight:
  1. Confirm arb still arms (edge % still above threshold).
  2. Re-fetch all 3 books; abort if any best-ask moved up or depth
     dropped below `1.5 × size`.
  3. Confirm per-market taker fees unchanged.
  4. Reserve `size × sum_asks` against the daily-spend rail.
- Execute: sign 3 FOK limit buys at the snapshot asks with the
  existing CLOB SDK path. Submit in parallel (`futures::join!`).
- **Outcome dispatch:**
  - All 3 filled → log `ARB OK`, stop.
  - Any leg returns partial or cancel → log `ARB PARTIAL` with
    filled-legs detail. **Do not auto-sell.** Emit a "stuck arb" card
    on `/arb` page with three buttons:
    - **Fill missing leg(s)** — places BUYs at current top-of-book on
      the unfilled outcome(s). Shows projected net P&L at current asks
      before confirm.
    - **Unwind filled legs** — reuses the existing Sell action (with
      90%-of-bid floor) to exit what we hold.
    - **Hold** — do nothing. Dismiss the card; position becomes a
      normal held position on the /trades Open Positions table.
- Arm / disarm: kill-switch on `/arb` page. Off by default on startup
  even with a CLI flag. Auto-disarm on any non-filled outcome until
  the user re-arms.

## UI surface

New `/arb` page with:

- **Live opportunities table** (Phase 1+):
  - Columns: Match · Kick-off · A Ask (depth) · Draw Ask (depth) · B Ask
    (depth) · Sum · Edge % · Max fillable · Fees · Arms?
  - Row highlight: green if Arms=YES, yellow if close-but-no, dim if
    no.
  - Phase 2: Paper-fire button per row (disabled when Arms=NO).
  - Phase 3: Execute button (disabled when engine disarmed).
- **Stuck arb recovery cards** (Phase 3):
  - One card per partial-fill entry.
  - Shows each leg: filled / unfilled, shares, avg price, current
    best ask / bid.
  - Three action buttons with projected P&L previewed on each.
- **Kill switch + daily spend bar** (Phase 3).

Navbar entry "Arb" added to all pages alongside Markets / Trades /
Weather / Observations / Paper Trades.

## Server state

```rust
struct ArbCandidate {
    event_id: String,           // Polymarket event slug
    match_title: String,
    kickoff: Option<DateTime<Utc>>,
    legs: [ArbLeg; 3],          // home / draw / away, in that order
    fetched_at: DateTime<Utc>,
}

struct ArbLeg {
    label: String,              // "Home" / "Draw" / "Away"
    market_title: String,       // full question text for log
    condition_id: String,
    token_id_yes: String,       // we buy YES on each
    best_ask: Option<Decimal>,
    ask_depth_shares: Option<Decimal>,
    best_bid: Option<Decimal>,
    taker_fee_rate: Decimal,    // from /markets/{cid}, 0 for zero-fee
}

struct ArbAnalysis {
    sum_asks: Decimal,
    fee_per_share_set: Decimal,
    net_cost_per_share: Decimal,
    edge_per_share: Decimal,
    edge_pct: Decimal,          // edge / cost
    max_fillable_shares: Decimal,
    arms: bool,
    blockers: Vec<String>,
}
```

Scanner runs on-demand per `/api/arb` request (no background task
required for Phase 1-2; polling cadence = whatever the UI refresh
sets, typically 10s).

## Out of scope

- Non-3-way arbs (e.g., over/under markets, handicap markets). Same
  math works for any N-outcome exhaustive set, but starting with the
  cleanest case keeps the group-by-event logic simple.
- Cross-sport arbs (tennis, cricket, etc.). Each sport needs its own
  title-pattern validation and resolution-rule review.
- Weather / crypto / politics arbs. Different question shapes; revisit
  later if we find they admit the same structure.
- Latency-sensitive fills (sub-second matching engine). We're fine
  with 1-2 second latency; if edges need to close faster than that,
  they're not edges we should be chasing with this architecture.
- Maker-side arb capture (posting asks inside the spread and waiting
  for takers). Completely different strategy, huge inventory risk.

## Status

- Phase 1: **in progress** (current PR).
- Phase 2: **in progress** (current PR).
- Phase 3: **deferred** — lands separately after Phase 2 runs clean
  for ≥ 1 full day of scanner operation.
