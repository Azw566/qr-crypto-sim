import numpy as np                                        # all the distributional math
import pandas as pd                                       # just for the tidy binned-imbalance table
from book import read_records, Book, in_order, is_first  # reuse the reconstruction primitives


def collect(path, symbol="btcusdt", venue="spot"):       # one pass: rebuild the book, sample its state over time
    depth = f"{symbol}@depth@100ms"                       # the L2 diff stream
    trade = f"{symbol}@trade"                             # the public-trade stream
    snap_name = f"{symbol}@snapshot"                      # the seed-snapshot record
    book = Book()                                         # live book
    seeded = started = False                              # consumed snapshot? locked onto the diff sequence?
    seg = 0                                               # contiguous-segment id; bumped on every gap

    bE, bid_px, mid, spr, bq, aq, bseg = [], [], [], [], [], [], []   # per-book-update samples
    tE, tsign, tseg = [], [], []                          # per-trade samples (exch time, signed dir, segment)

    for rec in read_records(path):                        # walk the (corruption-tolerant) log once
        name, d = rec["s"], rec["d"]                      # stream name and payload
        if name == snap_name and not seeded:              # seed snapshot for our symbol
            book.seed(d); seeded = True; continue
        if name == trade and seeded:                      # a public trade
            tsign.append(1 if not d["m"] else -1)         # m=False => buyer is taker => +1 (uptick-initiated)
            tE.append(d["E"]); tseg.append(seg)           # m=True  => seller is taker => -1
            continue
        if name != depth or not seeded:                   # ignore other symbols / pre-seed diffs
            continue
        if not started:                                   # straddle-and-lock onto the diff sequence
            if d["u"] <= book.last_u: continue
            if not is_first(venue, d, book.last_u): continue
            book.apply(d); started = True; continue
        if not in_order(venue, d, book.last_u):           # continuity broke -> a recording/sequence gap
            seg += 1                                       # start a new contiguous segment (no stats span this)
        book.apply(d)                                      # advance the book to this update
        bb, ba = book.best_bid(), book.best_ask()         # touch prices after applying
        if bb is None or ba is None:                      # degenerate empty side -> skip the sample
            continue
        bE.append(d["E"]); bid_px.append(bb)              # record the state of the touch
        mid.append((bb + ba) / 2); spr.append(ba - bb)
        bq.append(book.bids[bb]); aq.append(book.asks[ba])
        bseg.append(seg)

    return {                                              # hand back plain numpy arrays
        "bE": np.array(bE), "bid_px": np.array(bid_px), "mid": np.array(mid),
        "spr": np.array(spr), "bq": np.array(bq), "aq": np.array(aq), "bseg": np.array(bseg),
        "tE": np.array(tE), "tsign": np.array(tsign), "tseg": np.array(tseg), "segments": seg + 1,
    }


def infer_tick(prices):                                   # smallest price increment actually seen at the touch
    u = np.unique(prices)                                  # distinct best-bid prices
    diffs = np.diff(u)                                     # gaps between adjacent distinct prices
    return float(diffs[diffs > 0].min()) if diffs.size else float("nan")  # the minimum positive one


def autocorr(x, lags):                                    # autocorrelation of a 1-D series at given lags
    x = x - x.mean()                                       # de-mean
    v = np.dot(x, x)                                       # variance * n (lag-0)
    return {k: float(np.dot(x[:-k], x[k:]) / v) for k in lags}  # normalized lag-k autocovariance


def report(path, symbol="btcusdt", venue="spot"):         # compute and print the stylized-facts card
    d = collect(path, symbol, venue)                       # gather the samples
    tick = infer_tick(d["bid_px"])                         # price grid resolution
    span_h = (d["bE"][-1] - d["bE"][0]) / 3.6e6            # observed hours of book data

    spr_ticks = d["spr"] / tick                            # spread measured in ticks
    imb = (d["bq"] - d["aq"]) / (d["bq"] + d["aq"])        # queue imbalance in [-1, 1] at the touch

    # imbalance -> next mid move (the core microstructure signal), within a segment only
    nxt = np.full(d["mid"].size, np.nan)                   # next-event mid change in ticks
    same = d["bseg"][:-1] == d["bseg"][1:]                 # are i and i+1 in the same contiguous segment?
    nxt[:-1][same] = (d["mid"][1:][same] - d["mid"][:-1][same]) / tick  # signed move to the next event
    bins = np.linspace(-1, 1, 11)                          # 10 imbalance buckets
    tbl = pd.DataFrame({"imb": imb, "nxt": nxt}).dropna()  # drop the segment-boundary / tail rows
    tbl["bucket"] = pd.cut(tbl["imb"], bins)               # assign each sample to an imbalance bucket
    curve = tbl.groupby("bucket", observed=True)["nxt"].agg(["mean", "count"])  # E[next move | imbalance]

    # inter-trade durations, never measured across a gap
    dt = np.diff(d["tE"]) / 1000.0                          # seconds between consecutive trades
    dt = dt[d["tseg"][:-1] == d["tseg"][1:]]               # keep only within-segment gaps
    dt = dt[dt >= 0]                                        # guard against any clock wobble

    ac = autocorr(d["tsign"].astype(float), [1, 2, 5, 10, 50, 100])  # trade-sign autocorrelation

    print(f"=== {symbol} {venue} | {span_h:.2f}h | {d['segments']} segment(s) | tick={tick:g} ===")
    print(f"book updates: {d['mid'].size:,}   trades: {d['tsign'].size:,}")
    print("\n-- spread --")
    print(f"  median: {np.median(d['spr']):.2f}  ({np.median(spr_ticks):.2f} ticks)   "
          f"mean: {d['spr'].mean():.3f}   at 1 tick: {100*np.mean(spr_ticks <= 1.0 + 1e-6):.1f}%   "
          f"(large-tick if high)")
    print("\n-- best-queue size (base units) --")
    for q, lbl in ((d["bq"], "bid"), (d["aq"], "ask")):
        print(f"  {lbl}: median {np.median(q):.3f}   p10 {np.percentile(q,10):.3f}   p90 {np.percentile(q,90):.3f}")
    print("\n-- imbalance -> next-event mid move (ticks) --")
    print(curve.to_string())                               # should rise monotonically with imbalance
    print("\n-- inter-trade duration (s) --")
    print(f"  median: {np.median(dt):.3f}   mean: {dt.mean():.3f}   "
          f"CV: {dt.std()/dt.mean():.2f}   (CV>1 => clustered)")
    print("\n-- trade-sign autocorrelation --")
    for k, v in ac.items():
        print(f"  lag {k:>3}: {v:+.3f}")
    print("  (slow positive decay => long memory of order flow)")


if __name__ == "__main__":                                # run against the Jun 17 spot sample
    report("data/spot_20260617.jsonl.gz")
