import gzip
import io
import json
import zlib
from dataclasses import dataclass, field


def read_records(path):
    raw = open(path, "rb").read()
    n = len(raw)
    pos = 0
    buf = b""
    while pos < n:
        gz = gzip.GzipFile(fileobj=io.BytesIO(raw[pos:]))
        try:
            while True:
                chunk = gz.read(1 << 20)
                if not chunk:
                    return
                buf += chunk
                *lines, buf = buf.split(b"\n")
                for ln in lines:
                    if ln.strip():
                        yield json.loads(ln)
        except (EOFError, zlib.error, OSError):
            nxt = raw.find(b"\x1f\x8b", pos + 2)
            if nxt < 0:
                return
            pos = nxt
            buf = b""


@dataclass
class Book:
    bids: dict = field(default_factory=dict)
    asks: dict = field(default_factory=dict)
    bid_u: dict = field(default_factory=dict)
    ask_u: dict = field(default_factory=dict)
    last_u: int = 0

    def seed(self, snap):
        self.bids = {float(p): float(q) for p, q in snap["bids"]}
        self.asks = {float(p): float(q) for p, q in snap["asks"]}
        self.last_u = snap["lastUpdateId"]
        self.bid_u = {p: self.last_u for p in self.bids}
        self.ask_u = {p: self.last_u for p in self.asks}

    def apply_side(self, side, stamp, levels, u):
        for p, q in levels:
            price, qty = float(p), float(q)
            if qty == 0.0:
                side.pop(price, None)
                stamp.pop(price, None)
            else:
                side[price] = qty
                stamp[price] = u

    def prune(self):
        while self.bids and self.asks:
            bb, ba = max(self.bids), min(self.asks)
            if ba > bb:
                return
            if self.bid_u.get(bb, 0) <= self.ask_u.get(ba, 0):
                self.bids.pop(bb, None); self.bid_u.pop(bb, None)
            else:
                self.asks.pop(ba, None); self.ask_u.pop(ba, None)

    def apply(self, ev):
        u = ev["u"]
        self.apply_side(self.bids, self.bid_u, ev["b"], u)
        self.apply_side(self.asks, self.ask_u, ev["a"], u)
        self.last_u = u
        self.prune()

    def trim(self, cap=2000):
        if len(self.bids) > cap:
            keep = sorted(self.bids, reverse=True)[: cap * 3 // 4]
            self.bids = {p: self.bids[p] for p in keep}
            self.bid_u = {p: self.bid_u[p] for p in keep if p in self.bid_u}
        if len(self.asks) > cap:
            keep = sorted(self.asks)[: cap * 3 // 4]
            self.asks = {p: self.asks[p] for p in keep}
            self.ask_u = {p: self.ask_u[p] for p in keep if p in self.ask_u}

    def best_bid(self):
        return max(self.bids) if self.bids else None
    def best_ask(self):
        return min(self.asks) if self.asks else None
    def spread(self):
        b, a = self.best_bid(), self.best_ask()
        return None if b is None or a is None else a - b


def in_order(venue, ev, prev_u):
    if venue == "perp":
        return ev["pu"] == prev_u
    return ev["U"] == prev_u + 1


def is_first(venue, ev, snap_id):
    if venue == "perp":
        return ev["U"] <= snap_id <= ev["u"]
    return ev["U"] <= snap_id + 1 <= ev["u"]


def reconstruct(path, venue, symbol):
    depth = f"{symbol}@depth@100ms"
    snap_name = f"{symbol}@snapshot"
    book = Book()
    seeded = False
    started = False
    applied = gaps = stale = 0

    for rec in read_records(path):
        name, d = rec["s"], rec["d"]
        if name == snap_name and not seeded:
            book.seed(d)
            seeded = True
            continue
        if name != depth or not seeded:
            continue

        if not started:
            if d["u"] <= book.last_u:
                stale += 1
                continue
            if not is_first(venue, d, book.last_u):
                stale += 1
                continue
            book.apply(d)
            started = True
            applied += 1
            continue

        if not in_order(venue, d, book.last_u):
            gaps += 1
        book.apply(d)
        applied += 1

    return book, {"applied": applied, "gaps": gaps, "dropped_stale": stale}


if __name__ == "__main__":
    path, venue, symbol = "data/spot_20260617.jsonl.gz", "spot", "btcusdt"
    book, stats = reconstruct(path, venue, symbol)
    print(symbol, venue, stats)
    print("best bid/ask:", book.best_bid(), book.best_ask(), "spread:", book.spread())
    print("levels: %d bids / %d asks" % (len(book.bids), len(book.asks)))
