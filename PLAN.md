# Surebet Feature Plan — Polymarket, Chainlink, and FPGA Edge

## Current State (Phase 0 — Complete)

Native Rust binary with:
- **CLOB WebSocket connector** — streams order book snapshots, price changes, last trade prices from `wss://ws-subscriptions-clob.polymarket.com/ws/market`
- **RTDS WebSocket connector** — streams trade executions, order matches, crypto prices from `wss://ws-live-data.polymarket.com`
- **Local order book manager** — BTreeMap-based book with incremental updates, spread/depth calculations
- **Market discovery** — Gamma API integration to find and filter active markets by volume, liquidity, and category
- **Edge profiling** — classifies markets as DEEP/COMPETITIVE, MEDIUM/OPPORTUNITY, THIN/FRAGILE, or EVENT/DRIVEN
- **Config system** — TOML config + env vars for secrets, no credentials committed

---

## Where Low Depth Markets Don't Have an Edge

Research findings on which markets to avoid vs. target:

### Avoid (Low Edge, Thin Books)
- **Niche culture/entertainment** (Oscars, movie outcomes) — wide spreads but tiny fills, not worth the infrastructure
- **Weather markets** — novelty, near-zero participation
- **Long-tail altcoin predictions** — 63% of short-term crypto markets have zero 24h volume
- **Obscure global events** — minimal volume, no informational edge
- **Any market with <$100 liquidity** — 22.9% of all markets; price impact destroys edge

### Target (Latency/Signal Edge Possible)
- **15-minute crypto price markets** — Deep liquidity, Chainlink-resolved, but Polymarket added dynamic taker fees (peaks ~3.15% at 50/50) specifically to kill pure latency arb. Edge requires *signal* (oracle price prediction) not just speed.
- **Sports with liquid books** — Super Bowl, NBA Finals, etc. Rapid price moves on events, but need real-time sports data feeds for signal.
- **Major politics** — Deepest liquidity overall ($28M+ avg volume), but long-term positioning. Edge comes from information processing, not latency.
- **Event-driven markets near expiry** — High volume, draining liquidity. Mispricing during resolution windows is the real opportunity.

### Key Insight
Pure latency arbitrage on Polymarket is substantially dead due to the dynamic fee model. The edge now requires:
1. **Signal superiority** — know the outcome before the market prices it
2. **Oracle front-running** — for Chainlink-resolved markets, process the same data feeds Chainlink uses and position before settlement
3. **Cross-market arbitrage** — same event priced differently across Polymarket binary outcomes (YES + NO should sum to $1.00 minus fees)

---

## Phase 1 — L2 Auth & Order Placement

**Goal:** Execute trades programmatically.

- [ ] Implement HMAC-SHA256 L2 authentication for CLOB API
- [ ] Build order placement module (limit orders via POST /order)
- [ ] Build batch order support (up to 15 per POST /orders)
- [ ] Implement cancel operations (single, batch, cancel-all, cancel-market)
- [ ] Wire up the HeartBeats dead-man's-switch API for crash safety
- [ ] Add position tracking (GET /orders, GET /trades)
- [ ] Implement risk limits (max position size, max open orders, daily loss limit)

---

## Phase 2 — Cross-Outcome Arbitrage Engine

**Goal:** Detect and exploit mispricings within Polymarket itself.

- [ ] Monitor YES/NO token pairs — prices should sum to ~$1.00
- [ ] Calculate implied probability gaps accounting for taker fees
- [ ] Build an execution engine that simultaneously buys underpriced YES and NO when the sum < $1.00 - fees
- [ ] Track maker rebates (100% of taker fees redistributed to makers daily)
- [ ] Strategy: Post passive maker orders on both sides, collect rebates + capture spread
- [ ] Add multi-outcome market support (markets with >2 outcomes where sum of all outcome prices should equal $1.00)

---

## Phase 3 — Chainlink Oracle Integration

**Goal:** Front-run Chainlink-resolved market settlements using the same underlying data.

### Architecture
```
[Chainlink Data Streams] --> [Our Feed Parser] --> [Position Before Settlement]
       |                                                     |
       v                                                     v
[Chainlink Automation] --> [On-chain Settlement]     [Our Trade Submitted]
```

- [ ] Subscribe to Chainlink Data Streams for BTC/ETH/etc. price feeds
- [ ] Parse oracle reports — extract timestamped price, confidence interval, deviation threshold
- [ ] Build a prediction model: given current oracle price + deviation, what will Chainlink report at market expiry?
- [ ] For 15-minute crypto markets: compute expected settlement price 1-5 seconds before automation triggers
- [ ] Execute on Polymarket CLOB: buy YES or NO based on predicted settlement
- [ ] Key constraint: dynamic taker fee at ~3.15% means you need >3.15% edge to profit as a taker; maker orders pay 0% but risk being picked off

### Data Sources to Integrate
- Chainlink Data Streams (primary oracle data)
- Binance/Coinbase WebSocket feeds (leading indicators — exchanges often move 50-200ms before Chainlink updates)
- Mempool monitoring on Polygon (see pending Chainlink Automation transactions before they confirm)

---

## Phase 4 — Latency Optimization (Software)

**Goal:** Minimize tick-to-trade latency in the software stack.

- [ ] Profile the full pipeline: WebSocket receive → parse → decision → order submit → ack
- [ ] Replace serde_json with simd-json for parsing hot path
- [ ] Pre-serialize order templates (only fill in price/size at execution time)
- [ ] Use kernel bypass networking (io_uring or DPDK) for the WebSocket connections
- [ ] Implement connection pooling — maintain pre-authenticated HTTPS connections to CLOB API
- [ ] Deploy on Equinix NY4 or Amsterdam co-location (target <5ms to Polymarket infra)
- [ ] Add hardware timestamping for latency measurement (NIC-level timestamps)
- [ ] Build a latency dashboard: p50/p95/p99 for each pipeline stage

---

## Phase 5 — FPGA Exploration

**Goal:** Evaluate whether FPGA acceleration provides meaningful edge on prediction markets.

### Software FPGA Simulation (Before Hardware)
- [ ] Build a cycle-accurate simulation of the critical path in software
- [ ] Model: WebSocket frame parse → order book update → signal check → order construction
- [ ] Estimate achievable latency with FPGA vs. optimized software (realistic target: 1-5μs FPGA vs. 50-200μs software)
- [ ] Cost-benefit analysis: FPGA dev cost vs. expected alpha

### Hardware FPGA (If Simulation Justifies)
- [ ] Target board: Xilinx Alveo U50/U250 or Intel Agilex (PCIe-attached, 100GbE capable)
- [ ] Implement WebSocket frame parser in FPGA fabric (skip OS network stack entirely)
- [ ] Implement JSON field extraction in hardware (fixed schema → no need for general JSON parser)
- [ ] Implement order book update logic in FPGA (BTreeMap equivalent using block RAM)
- [ ] Implement signal logic (price threshold comparisons, cross-outcome sum check)
- [ ] Implement order construction and TCP transmission from FPGA

### Reality Check on FPGAs for Prediction Markets
FPGAs dominate in traditional HFT because:
- Exchange co-location puts you meters from the matching engine
- Nanosecond differences matter when thousands of firms compete
- Order flow is continuous and high-frequency

Polymarket is different:
- WebSocket delivery latency is ~50ms with high variance
- Order matching is off-chain (centralized operator) — no co-location program
- Event-driven price moves (news breaks) have seconds of reaction time, not microseconds
- The real bottleneck is *signal quality*, not execution speed

**Recommendation:** FPGAs make sense only for 15-minute crypto markets where Chainlink oracle data creates a tight, time-bounded signal. For all other market types, software optimization to sub-millisecond is more than sufficient.

### Alternative: GPU Acceleration
- [ ] Evaluate CUDA/OpenCL for parallel signal processing across hundreds of markets
- [ ] Monte Carlo simulation of multi-outcome market pricing
- [ ] Real-time NLP processing of news feeds (information edge > latency edge)

---

## Phase 6 — Information Edge Infrastructure

**Goal:** Build signal superiority rather than relying solely on speed.

- [ ] Integrate news/social media feeds (Twitter/X API, RSS aggregators)
- [ ] Build keyword extraction and entity linking for market-relevant events
- [ ] Implement a simple Bayesian model: P(outcome | news_event) → trade signal
- [ ] Monitor Polymarket comment feeds via RTDS for crowd sentiment shifts
- [ ] Track large order flow patterns (whale detection via RTDS trade feed)
- [ ] Build an alert system for resolution-window events (UMA proposal → dispute → vote)

---

## Architecture Target State

```
┌─────────────────────────────────────────────────────┐
│                    Signal Layer                       │
│  [Chainlink Feeds] [Exchange Feeds] [News/Social]    │
│  [Mempool Monitor] [Whale Tracker] [Sentiment]       │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│                  Decision Engine                      │
│  [Cross-Outcome Arb] [Oracle Front-run] [Signal MM]  │
│  [Risk Manager] [Position Tracker] [PnL Monitor]     │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│                 Execution Layer                       │
│  [CLOB WS Client] [Order Manager] [Batch Orders]    │
│  [HeartBeat DMS] [Latency Monitor] [FPGA Bypass]    │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│                  Market Data Layer                    │
│  [CLOB WS] [RTDS WS] [Order Book Store]             │
│  [Market Discovery] [Edge Profiler]                  │
└─────────────────────────────────────────────────────┘
```

---

## Priority Order

1. **Phase 2 (Cross-Outcome Arb)** — Lowest risk, mechanical edge, can run passively as maker
2. **Phase 1 (L2 Auth)** — Required for any execution
3. **Phase 3 (Chainlink Oracle)** — Highest potential alpha for crypto markets
4. **Phase 6 (Information Edge)** — Scales across all market types
5. **Phase 4 (Latency Optimization)** — Diminishing returns given Polymarket's architecture
6. **Phase 5 (FPGA)** — Only justified for crypto markets with proven Chainlink signal
