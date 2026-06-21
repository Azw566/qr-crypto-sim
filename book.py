import gzip                                              # raw logs are gzip-compressed jsonl
import json                                              # each line is one JSON record
from dataclasses import dataclass, field                


def read_records(path):                                 # stream records from one .jsonl.gz log
    with gzip.open(path, "rt") as f:                    # text mode: yields decoded str lines
        try:                                            # the recorder may have been hard-killed,
            for line in f:                              # leaving the final gzip member truncated;
                yield json.loads(line)                  # parse and hand back one record at a time
        except EOFError:                                # truncated tail -> stop cleanly,
            return                                      # everything before the last flush is valid


@dataclass                                              # mutable container for one symbol's book
class Book:
    bids: dict = field(default_factory=dict)            # price(float) -> resting quantity(float)
    asks: dict = field(default_factory=dict)            # price(float) -> resting quantity(float)
    last_u: int = 0                                     # final update id of the last applied event

    def seed(self, snap):                               # initialise from a REST depth snapshot
        self.bids = {float(p): float(q) for p, q in snap["bids"]}   # snapshot bid levels
        self.asks = {float(p): float(q) for p, q in snap["asks"]}   # snapshot ask levels
        self.last_u = snap["lastUpdateId"]              # the sequence id this snapshot is current to

    def apply_side(self, side, levels):                 # apply one side's price/qty deltas
        for p, q in levels:                             # each level is [price_str, qty_str]
            price, qty = float(p), float(q)             # Binance sends decimal strings
            if qty == 0.0:                              # zero quantity means the level is now empty
                side.pop(price, None)                   # remove it (may already be absent)
            else:                                       # otherwise the diff is an absolute new size,
                side[price] = qty                       # not an increment -> overwrite

    def apply(self, ev):                                # apply one depthUpdate event to the book
        self.apply_side(self.bids, ev["b"])            # b = bid-side level updates
        self.apply_side(self.asks, ev["a"])            # a = ask-side level updates
        self.last_u = ev["u"]                          # advance our sequence cursor

    def best_bid(self):                                 # highest price anyone is willing to buy at
        return max(self.bids) if self.bids else None    # None if the side is somehow empty
    def best_ask(self):                                # lowest price anyone is willing to sell at
        return min(self.asks) if self.asks else None
    def spread(self):                                  # ask - bid; the cost of crossing the book
        b, a = self.best_bid(), self.best_ask()
        return None if b is None or a is None else a - b


def in_order(venue, ev, prev_u):                        # is ev contiguous with the last applied event?
    if venue == "perp":                                 # futures stream chains via pu:
        return ev["pu"] == prev_u                       # this event's "previous u" must match our cursor
    return ev["U"] == prev_u + 1                         # spot stream chains via U == prev final + 1


def is_first(venue, ev, snap_id):                       # does ev straddle the snapshot's sequence id?
    if venue == "perp":                                 # futures: U <= snapId <= u
        return ev["U"] <= snap_id <= ev["u"]
    return ev["U"] <= snap_id + 1 <= ev["u"]            # spot: U <= snapId+1 <= u


def reconstruct(path, venue, symbol):                   # replay one symbol's diffs into a live book
    depth = f"{symbol}@depth@100ms"                     # the diff stream name for this symbol
    snap_name = f"{symbol}@snapshot"                    # the snapshot record name for this symbol
    book = Book()                                       # empty book to be seeded then driven
    seeded = False                                      # have we consumed the seed snapshot yet?
    started = False                                     # have we applied the first straddling event?
    applied = gaps = stale = 0                          # counters for the sync report

    for rec in read_records(path):                      # walk the log once, in record order
        name, d = rec["s"], rec["d"]                    # stream/snapshot name and its payload
        if name == snap_name and not seeded:            # the seed snapshot for our symbol,
            book.seed(d)                                # initialise the book from it
            seeded = True                               # and start looking for the first diff
            continue
        if name != depth or not seeded:                 # ignore other symbols, trades, pre-seed diffs
            continue

        if not started:                                 # still hunting for the event that straddles
            if d["u"] <= book.last_u:                   # fully before the snapshot -> already included
                stale += 1                              # count it and drop it
                continue
            if not is_first(venue, d, book.last_u):     # not yet the straddling event -> keep waiting
                stale += 1
                continue
            book.apply(d)                               # first valid event: apply and lock on
            started = True                               # subsequent events now checked for continuity
            applied += 1                                 # count it
            continue

        if not in_order(venue, d, book.last_u):         # continuity broke -> we missed update(s)
            gaps += 1                                    # record the gap (don't silently corrupt)
        book.apply(d)                                   # apply regardless so we resync going forward
        applied += 1                                    # count an applied event

    return book, {"applied": applied, "gaps": gaps, "dropped_stale": stale}


if __name__ == "__main__":                              # quick smoke run against today's sample
    path, venue, symbol = "data/spot_20260617.jsonl.gz", "spot", "btcusdt"
    book, stats = reconstruct(path, venue, symbol)      # rebuild btcusdt spot book
    print(symbol, venue, stats)                         # sync report
    print("best bid/ask:", book.best_bid(), book.best_ask(), "spread:", book.spread())
    print("levels: %d bids / %d asks" % (len(book.bids), len(book.asks)))
