import glob
import numpy as np
import pandas as pd
from book import read_records, Book, in_order, is_first
from label import level_deltas

SYMBOL = "btcusdt"
VENUE = "spot"
DEPTH = f"{SYMBOL}@depth@100ms"
TRADE = f"{SYMBOL}@trade"
SNAP = f"{SYMBOL}@snapshot"

QMAX = 25.0
NB = 250
DQ = QMAX / NB


def bin_of(q):
    i = int(q / DQ)
    return i if i < NB else NB - 1


def calibrate():
    time_h = np.zeros(NB)
    flux = np.zeros(NB)
    mkt_n = np.zeros(NB)
    mkt_v = np.zeros(NB)
    mkt_qty = 0.0
    mkt_cnt = 0
    boundary = []

    for path in sorted(glob.glob(f"data/{VENUE}_*.jsonl.gz")):
        book = Book()
        seeded = started = False
        pending = []
        prevE = None
        prev_bb = prev_ba = None

        for rec in read_records(path):
            name, d = rec["s"], rec["d"]

            if name == SNAP and not seeded:
                book.seed(d)
                seeded = True
                continue
            if name == TRADE and seeded:
                pending.append((d["E"], "bid" if d["m"] else "ask", float(d["p"]), float(d["q"])))
                continue
            if name != DEPTH or not seeded:
                continue

            if not started:
                if d["u"] <= book.last_u:
                    continue
                if not is_first(VENUE, d, book.last_u):
                    continue
                book.apply(d)
                started = True
                prev_bb, prev_ba = book.best_bid(), book.best_ask()
                prevE = d["E"]
                continue

            gap = not in_order(VENUE, d, book.last_u)
            E = d["E"]
            bb, ba = book.best_bid(), book.best_ask()
            delta = level_deltas(book, d)

            exb = exa = 0.0
            nb_n = na_n = 0
            nb_v = na_v = 0.0
            keep = []
            for tr in pending:
                if tr[0] <= E:
                    if tr[1] == "bid" and tr[2] == bb:
                        exb += tr[3]; nb_n += 1; nb_v += tr[3]
                    elif tr[1] == "ask" and tr[2] == ba:
                        exa += tr[3]; na_n += 1; na_v += tr[3]
                    mkt_qty += tr[3]; mkt_cnt += 1
                else:
                    keep.append(tr)
            pending = keep

            if not gap and prevE is not None and bb is not None and ba is not None:
                dt = (E - prevE) / 1000.0
                if dt > 0:
                    time_h[bin_of(book.bids[bb])] += dt
                    time_h[bin_of(book.asks[ba])] += dt
                for code, price, q, ex, mn, mv in (
                    ("bid", bb, book.bids[bb], exb, nb_n, nb_v),
                    ("ask", ba, book.asks[ba], exa, na_n, na_v),
                ):
                    b = bin_of(q)
                    flux[b] += delta.get((code, price), 0.0) + ex
                    mkt_n[b] += mn
                    mkt_v[b] += mv

            book.apply(d)
            nb, na = book.best_bid(), book.best_ask()
            if not gap:
                if nb is not None and prev_bb is not None and nb != prev_bb and book.bids[nb] > 0:
                    boundary.append(book.bids[nb])
                if na is not None and prev_ba is not None and na != prev_ba and book.asks[na] > 0:
                    boundary.append(book.asks[na])
            prev_bb, prev_ba = nb, na
            prevE = E

    return {
        "time": time_h, "flux": flux, "mkt_n": mkt_n, "mkt_v": mkt_v,
        "aes": mkt_qty / mkt_cnt if mkt_cnt else float("nan"),
        "boundary": np.array(boundary),
    }


BW = 0.5


def rows(c, qmax=8.0):
    q = (np.arange(NB) + 0.5) * DQ
    g = np.floor(q / BW).astype(int)
    out = []
    for k in sorted(set(g)):
        if (k + 0.5) * BW > qmax:
            break
        m = g == k
        ts = c["time"][m].sum()
        if ts <= 0:
            continue
        out.append(((k + 0.5) * BW, ts, c["flux"][m].sum() / ts,
                    c["mkt_n"][m].sum() / ts, c["mkt_v"][m].sum() / ts))
    return out


def report(c):
    print(f"=== {SYMBOL} {VENUE} | mean market print {c['aes']:.4f} base units ===")
    print(f"best-queue time {c['time'].sum()/3600:.2f}h   market trades {int(c['mkt_n'].sum()):,}")
    print("\n-- identifiable QR rates by best-queue size (q in base units) --")
    print(f"{'q':>6} {'time_s':>10} {'net_LC/s':>10} {'mkt/s':>8} {'mkt_vol/s':>10}")
    print("        (net_LC>0 replenish, <0 deplete; zero-crossing = resting size)")
    for q, ts, nu, lam, mv in rows(c):
        print(f"{q:>6.2f} {ts:>10.1f} {nu:>+10.4f} {lam:>8.3f} {mv:>10.4f}")
    bd = c["boundary"]
    print(f"\n-- boundary (regeneration) queue size in base units, n={bd.size:,} --")
    if bd.size:
        print(f"  mean {bd.mean():.3f}   median {np.median(bd):.3f}   "
              f"p10 {np.percentile(bd,10):.3f}   p90 {np.percentile(bd,90):.3f}")


def persist(c):
    pd.DataFrame(rows(c, qmax=QMAX), columns=["q", "time_s", "net_lc", "mkt", "mkt_vol"]).to_parquet(
        f"calib_{VENUE}_{SYMBOL}.parquet", index=False)
    pd.DataFrame({"q": c["boundary"]}).to_parquet(
        f"calib_{VENUE}_{SYMBOL}_boundary.parquet", index=False)


if __name__ == "__main__":
    c = calibrate()
    report(c)
    persist(c)
