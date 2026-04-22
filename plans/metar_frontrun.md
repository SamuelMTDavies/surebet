# METAR Frontrun — capture dead brackets ahead of market reprice

## What arb we're doing

Polymarket temperature bracket markets resolve on a station's METAR
readings (aviationweather.gov, the same data Wunderground scrapes).
Each new METAR report that pushes `max_so_far` (HIGH market) or
`min_so_far` (LOW market) past a bracket boundary makes that bracket
guaranteed to resolve NO.

**NO token of a dead bracket pays $1 at resolution.** Any NO ask < $1
is free money, modulo liquidity and timing. Because Polymarket traders/
bots reprice off Wunderground which itself lags METAR by some minutes,
polling METAR directly gives us a window — sometimes seconds, sometimes
longer — where the dead bracket's NO side is still offered at stale
pre-flip asks.

## Core constraints

1. **No slippage.** Fills are at the snapshot best-ask price or nothing.
   Limit order = snapshot best ask. If the book moves up between read
   and match, we'd rather get zero fill than an inflated fill.
2. **Small, bounded size.** $10 per capture hard cap. Size =
   `min($10 / best_ask, depth_at_best_ask)`. No walking up levels.
3. **Cheap-only gate.** Don't capture if best_ask > configurable
   ceiling (default $0.50 = 100% return minimum). Protects against
   manipulated or stale high asks sitting on dead brackets.
4. **Manual trigger.** Alerts surface the opportunity; human clicks
   Capture. No background auto-fire in Phase 1.
5. **Rate-limit safe.** Single multi-station METAR request per poll
   (the API accepts `ids=A,B,C,…`). 2-minute cadence ⇒ ~0.5 req/min,
   well under the 100/min cap even during burst-polling.

## Design

### Watchlist

Auto-derive from the existing weather-markets discovery:

- On each `/api/observations` refresh, collect the set of ICAO codes
  for every weather event whose `target_date` is today-local at the
  station.
- Store as `Arc<RwLock<BTreeSet<String>>>` on AppState; poller reads
  this each tick.

### Poller

Background task:

```
loop {
    let icaos = state.metar_watch.read().await.clone();
    if icaos.is_empty() { sleep 30s; continue }

    let url = format!(
        "https://aviationweather.gov/api/data/metar?ids={}&format=json&hours=2",
        icaos.join(",")
    );
    let reports = http.get(url).send().await?.json::<Vec<MetarReport>>().await?;

    for icao in icaos {
        let station_reports = reports.iter().filter(|r| r.icao_id == icao);
        let agg_today = aggregate_by_local_day(station_reports, today_local, tz);
        diff_against_cache(icao, agg_today).await;
    }

    sleep 2 minutes;
}
```

`hours=2` keeps the payload small (only the last ~2 METARs per station)
— we don't need to re-aggregate the whole day on every poll, we just
need to catch *new* reports. Today's running max/min are already
tracked in the `/api/observations` path; the diff logic only needs the
latest reading to decide whether a new extreme has landed.

### Diff logic

For each station on each poll:

```
let prev = cache.get(icao);
let new_sample = most_recent_report_temp;

if new_sample > prev.max_so_far (HIGH):
    // new extreme — find dead brackets
    for bracket in station.high_brackets:
        if bracket.upper <= new_sample AND bracket not already in dead_set:
            emit_alert(DEAD, bracket, new_sample, timestamp);

if new_sample < prev.min_so_far (LOW):
    // symmetric for LOW markets
    ...
```

Alerts go into a ring buffer (last 100) and also into the activity log
as `strategy="METAR ALERT"` so they show up in the historical feed.

### Capture endpoint

`POST /api/metar-capture`:

```
{
  "token_id_no": "…",       // NO side of the newly-dead bracket
  "bracket_label": "55°F or below",
  "market": "Highest temperature in Seattle on April 22?",
  "max_capture_usd": "10",  // default $10
  "max_capture_ask": "0.50" // default $0.50
}
```

Server flow:

1. Fetch current CLOB book for `token_id_no`.
2. `best_ask = book.asks.best()?`.
3. If `best_ask > max_capture_ask` → return `{ok: false, reason: "ask
   above ceiling"}` with the numbers for the UI to show.
4. `depth = size_at_best_ask`.
5. `shares = min(max_capture_usd / best_ask, depth)`, rounded down to
   2dp (CLOB size precision).
6. `cost = shares * best_ask`. Reserve against `daily_spend` rail.
7. Live mode: `place_limit(token_id_no, price=best_ask, size=shares,
   BUY)`. Paper mode: skip, log projected.
8. Log activity entry `strategy="METAR CAPTURE"` with
   `buy_cost=cost`, `sell_revenue=shares` (assumed $1/share at
   resolution), `net_profit=shares-cost`.

### UI

New panel at the top of `/observations`:

- **"Just flipped DEAD" feed** — ring of last N alerts, most recent
  first. Each row:
  - Timestamp + seconds-since
  - Station + market + bracket label
  - New reading that flipped it (e.g., "max jumped to 58.1°F")
  - Current NO best ask + depth
  - "Capture" button (disabled if ask > ceiling, with tooltip)
- Click Capture → confirm modal:
  - Exact size (shares + USDC)
  - Expected payout ($1/share at resolution)
  - Expected net profit
  - "Execute" button fires `/api/metar-capture`

Defaults configurable via env or /admin (future):
- `METAR_POLL_INTERVAL_SECS` = 120
- `METAR_MAX_CAPTURE_USD` = 10
- `METAR_MAX_CAPTURE_ASK` = 0.50

### Safety

- Off by default for live orders — paper mode captures write the
  projected activity entry but place no real order. Environment flag
  `METAR_CAPTURE_ARMED` or a UI toggle arms it.
- Reuse existing `max_trade` and `max_daily` rails. $10 captures are
  well within both.
- Auto-disarm on first unexpected error class (403 geoblock,
  insufficient balance, rate-limit). User re-arms.

## Phase 2 (deferred)

Optional one-click "Auto-capture while armed" — when armed, fires
automatically on every newly-dead event that passes the ceiling gate.
Same exact execution path as manual; just skips the confirm step.
Requires:
- Explicit arm with cap preview.
- Per-session spend ceiling separate from daily cap.
- Audit log of every auto-fire decision, including skips and why.

Keep in Phase 2 until Phase 1 has produced a few weeks of clean
manual captures and we've validated the alert signal is real.

## Out of scope

- Parking a resting maker order inside the spread (different strategy,
  inventory risk).
- Alerting off forecast model changes (Open-Meteo nowcast) — not
  resolution-grade.
- Multi-cycle capture (re-firing into the same bracket after partial
  fill) — Phase 1 is one shot per alert; repeats go through new alerts
  as the book re-posts asks.
- Cross-market hedging (buy NO here, sell YES elsewhere) — simple
  single-leg capture first.

## Status

- Phase 1 (alert + manual capture): **in progress**.
- Phase 2 (auto-capture while armed): **deferred** until Phase 1 runs
  clean for a few weeks.
