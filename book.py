import gzip                                              # raw logs are gzip-compressed jsonl
import io                                                # wrap the byte buffer for streaming decompression
import json                                              # each line is one JSON record
import zlib                                              # gzip raises zlib.error on a corrupt member
from dataclasses import dataclass, field


def read_records(path):                                 # stream records from one .jsonl.gz log, corruption-tolerant
    raw = open(path, "rb").read()                       # the daily log is a sequence of concatenated gzip members;
    n = len(raw)                                         # a single corrupt member must not strand the valid ones after it
    pos = 0                                             # byte offset of the next member to try
    buf = b""                                           # carry partial trailing line across decompression chunks
    while pos < n:                                       # walk the file member-group by member-group
        gz = gzip.GzipFile(fileobj=io.BytesIO(raw[pos:]))   # decode from here until a member goes bad
        try:
            while True:                                 # pull decompressed bytes in chunks
                chunk = gz.read(1 << 20)                # 1 MB of decompressed text at a time
                if not chunk:                           # clean end of the readable run
                    return                              # everything decodable has been yielded
                buf += chunk                            # append and split into whole lines
                *lines, buf = buf.split(b"\n")          # keep the last (possibly partial) fragment in buf
                for ln in lines:                        # hand back each complete record
                    if ln.strip():                      # skip blank lines
                        yield json.loads(ln)
        except (EOFError, zlib.error, OSError):         # this member is corrupt -> resync to the next one
            nxt = raw.find(b"\x1f\x8b", pos + 2)        # scan forward to the next gzip magic (1f 8b)
            if nxt < 0:                                 # no more members -> we are done
                return
            pos = nxt                                   # restart decompression at the next member
            buf = b""                                   # drop the partial line spanning the corrupt seam


@dataclass                                              # mutable container for one symbol's book
class Book:
    bids: dict = field(default_factory=dict)            # price(float) -> resting quantity(float)
    asks: dict = field(default_factory=dict)            # price(float) -> resting quantity(float)
    bid_u: dict = field(default_factory=dict)           # price -> update id that last touched this bid level
    ask_u: dict = field(default_factory=dict)           # price -> update id that last touched this ask level
    last_u: int = 0                                     # final update id of the last applied event

    def seed(self, snap):                               # initialise from a REST depth snapshot
        self.bids = {float(p): float(q) for p, q in snap["bids"]}   # snapshot bid levels
        self.asks = {float(p): float(q) for p, q in snap["asks"]}   # snapshot ask levels
        self.last_u = snap["lastUpdateId"]              # the sequence id this snapshot is current to
        self.bid_u = {p: self.last_u for p in self.bids}   # every seeded level is current as of the snapshot
        self.ask_u = {p: self.last_u for p in self.asks}

    def apply_side(self, side, stamp, levels, u):       # apply one side's price/qty deltas, stamping each level
        for p, q in levels:                             # each level is [price_str, qty_str]
            price, qty = float(p), float(q)             # Binance sends decimal strings
            if qty == 0.0:                              # zero quantity means the level is now empty
                side.pop(price, None)                   # remove it (may already be absent)
                stamp.pop(price, None)                  # and forget when it was last seen
            else:                                       # otherwise the diff is an absolute new size,
                side[price] = qty                       # not an increment -> overwrite
                stamp[price] = u                        # record the update that set it (for stale-level pruning)

    def prune(self):                                    # an ask at/below a bid is economically impossible:
        while self.bids and self.asks:                  # the offending level is leftover deep-book data Binance
            bb, ba = max(self.bids), min(self.asks)     # updates lazily, so it lags reality -> drop the staler one
            if ba > bb:                                 # book no longer crossed -> done
                return
            if self.bid_u.get(bb, 0) <= self.ask_u.get(ba, 0):  # whichever touch level was set longer ago
                self.bids.pop(bb, None); self.bid_u.pop(bb, None)   # is the stale one -> remove it
            else:
                self.asks.pop(ba, None); self.ask_u.pop(ba, None)

    def apply(self, ev):                                # apply one depthUpdate event to the book
        u = ev["u"]                                     # this event's final update id stamps every level it sets
        self.apply_side(self.bids, self.bid_u, ev["b"], u)   # b = bid-side level updates
        self.apply_side(self.asks, self.ask_u, ev["a"], u)   # a = ask-side level updates
        self.last_u = u                                 # advance our sequence cursor
        self.prune()                                    # keep the book uncrossed by evicting stale levels

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
