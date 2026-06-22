import pandas as pd                                       # events.parquet is written via pandas/pyarrow
from book import read_records, Book, in_order, is_first  # reuse the sync primitives from the reconstructor


TRADE_WINDOW_MS = 2000                                   # how long (exchange ms) a trade stays eligible to
#                                                        # explain a later book decrease before we give up on it


def level_deltas(book, ev):                              # what changed, per price level, BEFORE applying ev
    out = []                                             # list of (side, price, change) tuples
    for code, levels in (("bid", ev["b"]), ("ask", ev["a"])):  # walk both sides of the diff
        side = book.bids if code == "bid" else book.asks       # the matching live dict on the book
        for p, q in levels:                             # each level is [price_str, new_qty_str]
            price, newq = float(p), float(q)            # Binance sends decimal strings
            oldq = side.get(price, 0.0)                 # resting size before this update (0 if level was empty)
            change = newq - oldq                        # signed change at this price
            if change != 0.0:                           # a restated-but-identical size carries no event
                out.append((code, price, change))       # positive => add, negative => removal
    return out                                          # caller classifies each change


def attribute(trades, side, price, E, need):            # consume buffered trades to explain a book decrease
    matched = 0.0                                       # how much of `need` we managed to back with real trades
    for tr in trades:                                   # FIFO over the eligible-trade buffer
        if matched >= need:                             # decrease already fully explained
            break                                       # stop early
        tE, tside, tprice, tq = tr                      # buffered trade: exch time, side hit, price, qty left
        if tside != side or tprice != price:            # a trade only explains a decrease on the side and
            continue                                    # price level it actually executed against
        if tE > E:                                      # this trade is newer than the depth window we're in,
            continue                                    # so it can't have caused this particular decrease yet
        take = min(tq, need - matched)                  # consume as much as this trade and the gap allow
        tr[3] -= take                                   # shrink the trade's remaining unexplained-by-book qty
        matched += take                                 # and credit it toward the decrease
    return matched                                      # residual (need - matched) is treated as a cancellation


def label(path, symbol="btcusdt", venue="spot"):        # turn one symbol's raw log into a labeled event stream
    depth = f"{symbol}@depth@100ms"                     # the L2 diff stream name
    trade = f"{symbol}@trade"                           # the public-trade stream name
    snap_name = f"{symbol}@snapshot"                    # the seed-snapshot record name
    book = Book()                                       # live book, seeded then driven by diffs
    seeded = started = False                            # consumed snapshot? locked onto the diff sequence?
    trades = []                                         # rolling buffer of recent trades: [E, side, price, qty]
    events = []                                         # output rows we will turn into events.parquet
    trade_vol = matched_vol = 0.0                       # totals for the missed-execution quality metric
    gaps = 0                                            # diff-continuity breaks (labeling is unreliable across one)

    for rec in read_records(path):                      # single pass over the log, in record order
        name, d, t = rec["s"], rec["d"], rec["t"]       # stream name, payload, local arrival timestamp (ns)

        if name == snap_name and not seeded:            # the seed snapshot for our symbol
            book.seed(d)                                # initialise the book
            seeded = True                               # and start watching for the first usable diff
            continue

        if name == trade and seeded:                    # a public trade: buffer it for later attribution
            side = "bid" if d["m"] else "ask"           # m=True => seller was taker, so a resting BID was hit;
            #                                           # m=False => buyer was taker, so a resting ASK was hit
            trades.append([d["E"], side, float(d["p"]), float(d["q"])])  # record it with its qty still unexplained
            trade_vol += float(d["q"])                  # count every traded unit toward the denominator
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
        trades[:] = [tr for tr in trades                # expire the buffer: drop trades too old to still be
                     if tr[0] >= E - TRADE_WINDOW_MS and tr[3] > 1e-12]  # relevant, or already fully explained

        for side, price, change in level_deltas(book, d):   # classify every level change in this update
            if change > 0.0:                            # size grew (or a new level appeared)
                events.append((t, E, "insert", side, price, change))    # a limit-order insertion
            else:                                       # size shrank at this level
                dec = -change                           # the magnitude of the decrease to be explained
                m = attribute(trades, side, price, E, dec)  # how much real trade volume backs it
                if m > 1e-12:                           # part (or all) of it was an execution
                    events.append((t, E, "market", side, price, m))     # a marketable-order hit
                    matched_vol += m                    # credit it toward the attributed-trade total
                resid = dec - m                         # whatever the trade stream could not explain
                if resid > 1e-12:                       # ... is taken to be a cancellation
                    events.append((t, E, "cancel", side, price, resid)) # a limit-order cancellation

        book.apply(d)                                   # advance the book now that the diff has been labeled

    df = pd.DataFrame(events, columns=["t", "E", "type", "side", "price", "qty"])  # assemble the event table
    out = f"events_{venue}_{symbol}.parquet"            # one parquet per symbol/venue for now
    df.to_parquet(out, index=False)                     # persist the labeled stream

    miss = 1.0 - (matched_vol / trade_vol if trade_vol else 0.0)  # fraction of trade volume with no book decrease
    return out, {                                       # report card for the week-2 exit criterion
        "events": len(df),                              # total labeled events
        "by_type": df["type"].value_counts().to_dict(), # insert / market / cancel breakdown
        "trade_vol": round(trade_vol, 4),               # total executed volume seen on the trade stream
        "matched_vol": round(matched_vol, 4),           # how much of it we tied to a book decrease
        "unattributed_trade_rate": round(miss, 4),      # the quality metric — want this < a few %
        "gaps": gaps,                                   # diff holes encountered
    }


if __name__ == "__main__":                              # smoke run against the Jun 17 spot sample
    out, stats = label("data/spot_20260617.jsonl.gz")   # label btcusdt spot end-to-end
    print("wrote", out)                                 # where the parquet landed
    for k, v in stats.items():                          # and the report
        print(f"  {k}: {v}")
