# Session changes — Jun 23–24, 2026

Launched Week 2 (event labeling) and Week 3 (stylized facts) work, started data
collection, and fixed two real reconstruction bugs found along the way.

## New files

### `label.py` — event labeling → `events.parquet`
Turns a raw daily log into a labeled event stream `events_<venue>_<symbol>.parquet`
(columns `t, E, type, side, price, qty`, sorted by arrival time `t`).
- **Model:** the `@trade` stream is ground truth for executions. Every trade becomes a
  per-trade `market` event (m=False → ask hit, m=True → bid hit). The book's net delta
  per level then splits the residual into `insert`/`cancel` via `inserts−cancels = net + execs`.
- **Use:** source material for Post 2 (LOB reconstruction) and the input event stream the
  QR simulator will be calibrated against.
- **Result on Jun 17:** 4.93M events, 619,499 market events = exact match to raw trade
  count and volume (0 executions lost). `book_corroborated_rate = 78.6%` is reported as a
  data-quality stat — the fraction of execution volume the 100 ms diff independently shows
  as a drop (the other 21.4% is hidden by same-window execute-and-refill, an honest limit
  of aggregated L2 feeds, not lost data).

### `validate.py` — stylized facts (first analysis)
Single pass that rebuilds the book and samples its state to produce the stylized-facts card:
spread distribution (abs + ticks, % at 1 tick), best-queue sizes, imbalance→next-move curve,
inter-trade durations, trade-sign autocorrelation. Time-based stats are computed only within
contiguous segments (reset on each gap) so nothing spans a recording hole.
- **Use:** Post 1 (what Binance's book looks like) and a sanity check before calibration.
- **Result on Jun 17:** BTC spot is strongly **large-tick** (spread = 1 tick ~97% of the time);
  imbalance→next-move is clean and monotonic (−34 to +22 ticks across the imbalance range);
  trade-sign autocorrelation +0.95→+0.61 over lags 1–100 (long-memory order flow);
  trade durations clustered (CV ≈ 6).

### `run.md` — recorder launch guide
How to run the data gatherer yourself as a persistent side process, keep it healthy
(one instance, no laptop sleep), check it's alive, and optionally auto-start it.

### `CHANGES.md` — this file.

## Modified files

### `book.py`
1. **Corruption-tolerant `read_records`.** Daily logs contain unreadable gzip seams (a
   `flush()`ed-but-never-`close()`d member followed by a fresh header — caused only by hard
   kills: sleep/force-quit/power loss). The reader now recovers member-by-member, skipping to
   the next gzip magic on error. Recovered records on Jun 17 went from ~2.8K → 2.48M (was
   silently dropping ~99% of the day). **Zero data lost** (label.py gets exact trade counts).
2. **Stale-level pruning (`Book` uncrossing).** Binance updates far-from-touch levels lazily,
   so deep levels lag and the reconstructed book showed crossed (negative) spreads. Each level
   now carries the update-id that last set it; `prune()` runs after every `apply()` and, while
   the book is crossed, evicts the **staler** of the two touch levels. Fixed: 0 negative
   spreads, median spread = 1 tick. Required for any touch-based statistic to be meaningful.
   - *Known cost:* `prune()` does `max`/`min` over the whole book each update, so a full-day
     pass takes a few minutes. Fine for prototyping; optimize (sorted container / incremental
     touch) if it bites.

## Operational
- **Data collection (re)started Jun 23**; only Jun 17 existed before (collection had stopped
  after one day). Recorder code itself is fine — see the hard-kill note above. Run it yourself
  per `run.md`.

## Removed
- `diag.py` — one-off diagnostic that confirmed the 21.4% hidden-execution rate was net-delta
  hiding (not a sync bug). Its headline number is now reported by `label.py`.
