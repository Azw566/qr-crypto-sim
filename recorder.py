import asyncio
import gzip
import json
import logging
import logging.handlers
import os
import signal
import time
import urllib.request
from datetime import datetime, timezone

import websockets

SYMBOLS = {
    "spot": ["btcusdt", "ethusdt", "solusdt", "bnbusdt"],
    "perp": ["btcusdt"],
}

ENDPOINTS = {
    "spot": "wss://stream.binance.com:9443/stream?streams=",
    "perp": "wss://fstream.binance.com/stream?streams=",
}

REST = {
    "spot": "https://api.binance.com/api/v3/depth",
    "perp": "https://fapi.binance.com/fapi/v1/depth",
}

DATA_DIR = "data"
RECONNECT_DELAY = 5
FLUSH_INTERVAL = 1.0     # seconds between gzip flushes (bounds data lost on kill)
SNAPSHOT_LIMIT = 1000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.handlers.RotatingFileHandler(
            "recorder.log", maxBytes=50_000_000, backupCount=5
        ),
    ],
)
log = logging.getLogger(__name__)

# keyed by (venue, stream) so spot/perp btcusdt don't interact with each other
last_u: dict[tuple[str, str], int] = {}


def check_gap(venue: str, stream: str, msg: dict) -> None:
    if msg.get("e") != "depthUpdate":
        return
    key = (venue, stream)
    prev = last_u.get(key)
    if venue == "perp":
        pu = msg.get("pu")
        if prev is not None and pu != prev:
            log.warning("GAP %s %s: prev u=%s got pu=%s", venue, stream, prev, pu)
    else:
        # spot continuity: this event's U must be previous u + 1 -> gap detection
        U = msg["U"]
        if prev is not None and U != prev + 1:
            log.warning(
                "GAP %s %s: expected U=%d got U=%d (missed %d updates)",
                venue, stream, prev + 1, U, U - prev - 1,
            )
    last_u[key] = msg["u"]


def build_stream_path(symbols: list[str]) -> str:
    parts = []
    for s in symbols:
        parts.append(f"{s}@depth@100ms")  # L2 diff stream at 100 ms granularity
        parts.append(f"{s}@trade")        
    return "/".join(parts)


def today_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")


def open_outfile(venue: str, date_str: str):
    os.makedirs(DATA_DIR, exist_ok=True)
    path = f"{DATA_DIR}/{venue}_{date_str}.jsonl.gz"
    return gzip.open(path, "at")


def _fetch_snapshot(venue: str, symbol: str) -> dict:
    url = f"{REST[venue]}?symbol={symbol.upper()}&limit={SNAPSHOT_LIMIT}"
    with urllib.request.urlopen(url, timeout=10) as r:
        return json.loads(r.read())


async def write_snapshots(venue: str, fh, reset: bool) -> None:
    for sym in SYMBOLS[venue]:
        if reset:
            last_u.pop((venue, f"{sym}@depth@100ms"), None)
        try:
            snap = await asyncio.to_thread(_fetch_snapshot, venue, sym)
            rec = {"t": time.time_ns(), "s": f"{sym}@snapshot", "d": snap}
            fh.write(json.dumps(rec) + "\n")
        except Exception as e:
            log.warning("snapshot failed %s %s: %s", venue, sym, e)
    fh.flush()


async def record_venue(venue: str) -> None:
    url = ENDPOINTS[venue] + build_stream_path(SYMBOLS[venue])

    while True:  # reconnect until stopped loop
        try:
            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=30,
            ) as ws:
                log.info("connected %s", venue)
                date_str = today_str()
                fh = open_outfile(venue, date_str)
                # snapshot AFTER the socket is open so buffered diffs cover the seam
                await write_snapshots(venue, fh, reset=True)
                last_flush = time.monotonic()

                try:
                    async for raw in ws:
                        now = time.time_ns()  # wall-clock ns; compare against Binance E

                        new_date = today_str()
                        if new_date != date_str:
                            fh.close()
                            date_str = new_date
                            fh = open_outfile(venue, date_str)
                            await write_snapshots(venue, fh, reset=False)

                        envelope = json.loads(raw)
                        stream_name = envelope.get("stream", "")
                        data = envelope.get("data", envelope)

                        check_gap(venue, stream_name, data)

                        record = {"t": now, "s": stream_name, "d": data}
                        fh.write(json.dumps(record) + "\n")

                        mono = time.monotonic()
                        if mono - last_flush >= FLUSH_INTERVAL:
                            fh.flush()
                            last_flush = mono

                except Exception as e:
                    log.error("stream error %s: %s", venue, e)
                finally:
                    fh.close()  # close() finalizes the gzip member and flushes

        except asyncio.CancelledError:
            log.info("shutting down %s", venue)
            raise
        except Exception as e:
            log.error("connection error %s: %s", venue, e)

        log.info("reconnecting %s in %ds …", venue, RECONNECT_DELAY)
        await asyncio.sleep(RECONNECT_DELAY)


async def main() -> None:
    loop = asyncio.get_running_loop()
    tasks = [asyncio.create_task(record_venue(v)) for v in SYMBOLS]

    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, AttributeError):
            pass  # Windows ProactorEventLoop: fall back to KeyboardInterrupt

    await stop.wait()
    log.info("signal received, draining …")
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
