import glob
import numpy as np
import pandas as pd
from book import read_records, Book, in_order, is_first

SYMBOLS = {
    "spot": ["btcusdt", "ethusdt", "solusdt", "bnbusdt"],
    "perp": ["btcusdt"],
}


def collect(venue, symbols=None):
    symbols = symbols or SYMBOLS[venue]
    routes = {}
    for s in symbols:
        routes[f"{s}@depth@100ms"] = (s, "depth")
        routes[f"{s}@trade"] = (s, "trade")
        routes[f"{s}@snapshot"] = (s, "snap")

    st = {s: {"book": Book(), "seeded": False, "started": False, "seg": 0} for s in symbols}
    rows = {s: {"bE": [], "bid_px": [], "mid": [], "spr": [], "bq": [], "aq": [], "bseg": [],
                "tE": [], "tsign": [], "tseg": []} for s in symbols}

    for path in sorted(glob.glob(f"data/{venue}_*.jsonl.gz")):
        for s in symbols:
            S = st[s]
            S["book"] = Book(); S["seeded"] = False; S["started"] = False
            S["seg"] += 1

        for rec in read_records(path):
            hit = routes.get(rec["s"])
            if hit is None:
                continue
            s, kind = hit
            S = st[s]
            d = rec["d"]

            if kind == "snap":
                if not S["seeded"]:
                    S["book"].seed(d); S["seeded"] = True
                continue
            if not S["seeded"]:
                continue
            if kind == "trade":
                rows[s]["tsign"].append(1 if not d["m"] else -1)
                rows[s]["tE"].append(d["E"]); rows[s]["tseg"].append(S["seg"])
                continue

            book = S["book"]
            if not S["started"]:
                if d["u"] <= book.last_u:
                    continue
                if not is_first(venue, d, book.last_u):
                    continue
                book.apply(d); S["started"] = True
                continue
            if not in_order(venue, d, book.last_u):
                S["seg"] += 1
            book.apply(d)
            book.trim()
            bb, ba = book.best_bid(), book.best_ask()
            if bb is None or ba is None:
                continue
            r = rows[s]
            r["bE"].append(d["E"]); r["bid_px"].append(bb)
            r["mid"].append((bb + ba) / 2); r["spr"].append(ba - bb)
            r["bq"].append(book.bids[bb]); r["aq"].append(book.asks[ba])
            r["bseg"].append(S["seg"])

    out = {}
    for s in symbols:
        d = {k: np.array(v) for k, v in rows[s].items()}
        d["segments"] = int(st[s]["seg"]) + 1
        out[s] = d
    return out


def persist(venue, data):
    for s, d in data.items():
        pd.DataFrame({
            "E": d["bE"], "bid_px": d["bid_px"], "mid": d["mid"],
            "spr": d["spr"], "bq": d["bq"], "aq": d["aq"], "seg": d["bseg"],
        }).to_parquet(f"samples_{venue}_{s}_book.parquet", index=False)
        pd.DataFrame({
            "E": d["tE"], "sign": d["tsign"], "seg": d["tseg"],
        }).to_parquet(f"samples_{venue}_{s}_trade.parquet", index=False)


def load(venue, symbol):
    b = pd.read_parquet(f"samples_{venue}_{symbol}_book.parquet")
    t = pd.read_parquet(f"samples_{venue}_{symbol}_trade.parquet")
    segs = max(int(b["seg"].max()) if len(b) else 0, int(t["seg"].max()) if len(t) else 0) + 1
    return {
        "bE": b["E"].to_numpy(), "bid_px": b["bid_px"].to_numpy(), "mid": b["mid"].to_numpy(),
        "spr": b["spr"].to_numpy(), "bq": b["bq"].to_numpy(), "aq": b["aq"].to_numpy(),
        "bseg": b["seg"].to_numpy(),
        "tE": t["E"].to_numpy(), "tsign": t["sign"].to_numpy(), "tseg": t["seg"].to_numpy(),
        "segments": segs,
    }


def infer_tick(prices):
    u = np.unique(prices)
    diffs = np.diff(u)
    return float(diffs[diffs > 0].min()) if diffs.size else float("nan")


def autocorr(x, lags):
    x = x - x.mean()
    v = np.dot(x, x)
    return {k: float(np.dot(x[:-k], x[k:]) / v) for k in lags}


def report(d, symbol="btcusdt", venue="spot"):
    if d["mid"].size == 0:
        print(f"=== {symbol} {venue} | no book data ===")
        return
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
    if dt.size:
        print(f"  median: {np.median(dt):.3f}   mean: {dt.mean():.3f}   "
              f"CV: {dt.std()/dt.mean():.2f}   (CV>1 => clustered)")
    else:
        print("  (no within-segment trade pairs)")
    print("\n-- trade-sign autocorrelation --")
    for k, v in ac.items():
        print(f"  lag {k:>3}: {v:+.3f}")
    print("  (slow positive decay => long memory of order flow)")


if __name__ == "__main__":
    for venue in SYMBOLS:
        data = collect(venue)
        persist(venue, data)
        for symbol, d in data.items():
            report(d, symbol, venue)
            print()
