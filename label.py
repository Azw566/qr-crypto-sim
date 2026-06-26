import pandas as pd
from book import read_records, Book, in_order, is_first


def level_deltas(book, ev):
    out = {}
    for code, levels in (("bid", ev["b"]), ("ask", ev["a"])):
        side = book.bids if code == "bid" else book.asks
        for p, q in levels:
            price, newq = float(p), float(q)
            oldq = side.get(price, 0.0)
            change = newq - oldq
            if change != 0.0:
                out[(code, price)] = change
    return out


def label(path, symbol="btcusdt", venue="spot"):
    depth = f"{symbol}@depth@100ms"
    trade = f"{symbol}@trade"
    snap_name = f"{symbol}@snapshot"
    book = Book()
    seeded = started = False
    pending = []
    events = []
    trade_vol = corroborated = 0.0
    gaps = 0

    for rec in read_records(path):
        name, d, t = rec["s"], rec["d"], rec["t"]

        if name == snap_name and not seeded:
            book.seed(d)
            seeded = True
            continue

        if name == trade and seeded:
            side = "bid" if d["m"] else "ask"
            pending.append([d["E"], side, float(d["p"]), float(d["q"]), t])
            trade_vol += float(d["q"])
            continue

        if name != depth or not seeded:
            continue

        if not started:
            if d["u"] <= book.last_u:
                continue
            if not is_first(venue, d, book.last_u):
                continue
            book.apply(d)
            started = True
            continue

        if not in_order(venue, d, book.last_u):
            gaps += 1

        E = d["E"]
        delta = level_deltas(book, d)

        execed = {}
        keep = []
        for tr in pending:
            if tr[0] <= E:
                events.append((tr[4], tr[0], "market", tr[1], tr[2], tr[3]))
                execed[(tr[1], tr[2])] = execed.get((tr[1], tr[2]), 0.0) + tr[3]
            else:
                keep.append(tr)
        pending = keep

        for key in delta.keys() | execed.keys():
            net = delta.get(key, 0.0)
            X = execed.get(key, 0.0)
            corroborated += min(X, max(0.0, -net))
            signed = net + X
            if signed > 1e-12:
                events.append((t, E, "insert", key[0], key[1], signed))
            elif signed < -1e-12:
                events.append((t, E, "cancel", key[0], key[1], -signed))

        book.apply(d)

    df = pd.DataFrame(events, columns=["t", "E", "type", "side", "price", "qty"])
    df = df.sort_values("t", kind="stable").reset_index(drop=True)
    out = f"events_{venue}_{symbol}.parquet"
    df.to_parquet(out, index=False)

    tail = sum(tr[3] for tr in pending)
    return out, {
        "events": len(df),
        "by_type": df["type"].value_counts().to_dict(),
        "trade_vol": round(trade_vol, 4),
        "book_corroborated_rate": round(corroborated / trade_vol, 4) if trade_vol else 0.0,
        "unattributed_vol": round(tail, 4),
        "gaps": gaps,
    }


if __name__ == "__main__":
    out, stats = label("data/spot_20260617.jsonl.gz")
    print("wrote", out)
    for k, v in stats.items():
        print(f"  {k}: {v}")
