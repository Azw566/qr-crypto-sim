import pandas as pd                                       # events.parquet is written via pandas/pyarrow
from book import read_records, Book, in_order, is_first  # reuse the sync primitives from the reconstructor


def level_deltas(book, ev):                              # what changed, per price level, BEFORE applying ev
    out = {}                                             # (side, price) -> signed change
    for code, levels in (("bid", ev["b"]), ("ask", ev["a"])):  # walk both sides of the diff
        side = book.bids if code == "bid" else book.asks       # the matching live dict on the book
        for p, q in levels:                             # each level is [price_str, new_qty_str]
            price, newq = float(p), float(q)            # Binance sends decimal strings
            oldq = side.get(price, 0.0)                 # resting size before this update (0 if level was empty)
            change = newq - oldq                        # signed change at this price
            if change != 0.0:                           # a restated-but-identical size carries no event
                out[(code, price)] = change             # positive => net add, negative => net removal
    return out                                          # caller reconciles these against executions


def label(path, symbol="btcusdt", venue="spot"):        # turn one symbol's raw log into a labeled event stream
    depth = f"{symbol}@depth@100ms"                     # the L2 diff stream name
    trade = f"{symbol}@trade"                           # the public-trade stream name
    snap_name = f"{symbol}@snapshot"                    # the seed-snapshot record name
    book = Book()                                       # live book, seeded then driven by diffs
    seeded = started = False                            # consumed snapshot? locked onto the diff sequence?
    pending = []                                        # trades awaiting their depth window: [E, side, price, qty, t]
    events = []                                         # output rows we will turn into events.parquet
    trade_vol = corroborated = 0.0                      # total executed volume, and how much the book confirmed
    gaps = 0                                            # diff-continuity breaks (labeling unreliable across one)

    for rec in read_records(path):                      # single pass over the log, in record order
        name, d, t = rec["s"], rec["d"], rec["t"]       # stream name, payload, local arrival timestamp (ns)

        if name == snap_name and not seeded:            # the seed snapshot for our symbol
            book.seed(d)                                # initialise the book
            seeded = True                               # and start watching for the first usable diff
            continue

        if name == trade and seeded:                    # a public trade: queue it for its depth window
            side = "bid" if d["m"] else "ask"           # m=True => seller was taker, so a resting BID was hit;
            #                                           # m=False => buyer was taker, so a resting ASK was hit
            pending.append([d["E"], side, float(d["p"]), float(d["q"]), t])  # exch time, side, price, qty, arrival
            trade_vol += float(d["q"])                  # the trade stream is ground truth for executions
            continue

        if name != depth or not seeded:                 # ignore other symbols / pre-seed diffs
            continue

        if not started:                                 # still hunting the diff that straddles the snapshot id
            if d["u"] <= book.last_u:                   # entirely before the snapshot -> already included
                continue                                # drop it
            if not is_first(venue, d, book.last_u):     # not yet the straddling event -> keep waiting
                continue
            book.apply(d)                               # first valid event: apply WITHOUT labeling (this is the lock)
            started = True                              # from here on, every diff is diffed and labeled
            continue

        if not in_order(venue, d, book.last_u):         # continuity broke -> we missed update(s)
            gaps += 1                                   # deltas around the hole will be wrong; flag, don't trust

        E = d["E"]                                      # exchange time of this depth update (ms)
        delta = level_deltas(book, d)                   # net book change per level over this 100 ms window

        execed = {}                                     # executions assigned to THIS window, summed per level
        keep = []                                       # trades that belong to a later window
        for tr in pending:                              # drain every trade whose exch time precedes this update
            if tr[0] <= E:                              # this trade executed within (or before) this window
                events.append((tr[4], tr[0], "market", tr[1], tr[2], tr[3]))  # emit it at its own real timing
                execed[(tr[1], tr[2])] = execed.get((tr[1], tr[2]), 0.0) + tr[3]  # tally execs at this level
            else:                                       # newer than this depth window -> defer it
                keep.append(tr)
        pending = keep                                  # only future-window trades remain queued

        for key in delta.keys() | execed.keys():        # reconcile every level touched by a diff OR a trade
            net = delta.get(key, 0.0)                   # observed net size change (0 if level absent from diff)
            X = execed.get(key, 0.0)                    # executions there this window (already emitted above)
            corroborated += min(X, max(0.0, -net))      # how much of the execution the book independently showed
            signed = net + X                            # net = inserts - cancels - execs  =>  inserts - cancels
            if signed > 1e-12:                          # the level grew once executions are accounted for
                events.append((t, E, "insert", key[0], key[1], signed))     # net limit-order insertion
            elif signed < -1e-12:                       # the level shrank beyond what executions explain
                events.append((t, E, "cancel", key[0], key[1], -signed))    # net limit-order cancellation

        book.apply(d)                                   # advance the book now that the window has been labeled

    df = pd.DataFrame(events, columns=["t", "E", "type", "side", "price", "qty"])  # assemble the event table
    df = df.sort_values("t", kind="stable").reset_index(drop=True)  # true observation order (markets at trade time)
    out = f"events_{venue}_{symbol}.parquet"            # one parquet per symbol/venue for now
    df.to_parquet(out, index=False)                     # persist the labeled stream

    tail = sum(tr[3] for tr in pending)                 # trades after the final depth window (never reconciled)
    return out, {                                       # report card for the week-2 exit criterion
        "events": len(df),                              # total labeled events
        "by_type": df["type"].value_counts().to_dict(), # insert / market / cancel breakdown
        "trade_vol": round(trade_vol, 4),               # total executed volume from the trade stream (all attributed)
        "book_corroborated_rate": round(corroborated / trade_vol, 4) if trade_vol else 0.0,  # data-quality stat:
        #                                               # fraction of executions the 100 ms diff also showed as a drop
        "unattributed_vol": round(tail, 4),             # executions we could NOT place in a window (want ~0)
        "gaps": gaps,                                   # diff holes encountered
    }


if __name__ == "__main__":                              # smoke run against the Jun 17 spot sample
    out, stats = label("data/spot_20260617.jsonl.gz")   # label btcusdt spot end-to-end
    print("wrote", out)                                 # where the parquet landed
    for k, v in stats.items():                          # and the report
        print(f"  {k}: {v}")
