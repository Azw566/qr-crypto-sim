import numpy as np
import pandas as pd
from book import read_records, Book, in_order, is_first


def collect(path, symbol="btcusdt", venue="spot"):
    depth = f"{symbol}@depth@100ms"
    trade = f"{symbol}@trade"
    snap_name = f"{symbol}@snapshot"
    book = Book()
    seeded = started = False
    seg = 0

    bE, bid_px, mid, spr, bq, aq, bseg = [], [], [], [], [], [], []
    tE, tsign, tseg = [], [], []

    for rec in read_records(path):
        name, d = rec["s"], rec["d"]
        if name == snap_name and not seeded:
            book.seed(d); seeded = True; continue
        if name == trade and seeded:
            tsign.append(1 if not d["m"] else -1)
            tE.append(d["E"]); tseg.append(seg)
            continue
        if name != depth or not seeded:
            continue
        if not started:
            if d["u"] <= book.last_u: continue
            if not is_first(venue, d, book.last_u): continue
            book.apply(d); started = True; continue
        if not in_order(venue, d, book.last_u):
            seg += 1
        book.apply(d)
        bb, ba = book.best_bid(), book.best_ask()
        if bb is None or ba is None:
            continue
        bE.append(d["E"]); bid_px.append(bb)
        mid.append((bb + ba) / 2); spr.append(ba - bb)
        bq.append(book.bids[bb]); aq.append(book.asks[ba])
        bseg.append(seg)

    return {
        "bE": np.array(bE), "bid_px": np.array(bid_px), "mid": np.array(mid),
        "spr": np.array(spr), "bq": np.array(bq), "aq": np.array(aq), "bseg": np.array(bseg),
        "tE": np.array(tE), "tsign": np.array(tsign), "tseg": np.array(tseg), "segments": seg + 1,
    }


def infer_tick(prices):
    u = np.unique(prices)
    diffs = np.diff(u)
    return float(diffs[diffs > 0].min()) if diffs.size else float("nan")


def autocorr(x, lags):
    x = x - x.mean()
    v = np.dot(x, x)
    return {k: float(np.dot(x[:-k], x[k:]) / v) for k in lags}


def report(path, symbol="btcusdt", venue="spot"):
    d = collect(path, symbol, venue)
    tick = infer_tick(d["bid_px"])
    span_h = (d["bE"][-1] - d["bE"][0]) / 3.6e6

    spr_ticks = d["spr"] / tick
    imb = (d["bq"] - d["aq"]) / (d["bq"] + d["aq"])

    nxt = np.full(d["mid"].size, np.nan)
    same = d["bseg"][:-1] == d["bseg"][1:]
    nxt[:-1][same] = (d["mid"][1:][same] - d["mid"][:-1][same]) / tick
    bins = np.linspace(-1, 1, 11)
    tbl = pd.DataFrame({"imb": imb, "nxt": nxt}).dropna()
    tbl["bucket"] = pd.cut(tbl["imb"], bins)
    curve = tbl.groupby("bucket", observed=True)["nxt"].agg(["mean", "count"])

    dt = np.diff(d["tE"]) / 1000.0
    dt = dt[d["tseg"][:-1] == d["tseg"][1:]]
    dt = dt[dt >= 0]

    ac = autocorr(d["tsign"].astype(float), [1, 2, 5, 10, 50, 100])

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
    print(curve.to_string())
    print("\n-- inter-trade duration (s) --")
    print(f"  median: {np.median(dt):.3f}   mean: {dt.mean():.3f}   "
          f"CV: {dt.std()/dt.mean():.2f}   (CV>1 => clustered)")
    print("\n-- trade-sign autocorrelation --")
    for k, v in ac.items():
        print(f"  lag {k:>3}: {v:+.3f}")
    print("  (slow positive decay => long memory of order flow)")


if __name__ == "__main__":
    report("data/spot_20260617.jsonl.gz")
