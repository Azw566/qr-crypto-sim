import asyncio        
import gzip           
import json           
import logging        
import os            
import time        
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

DATA_DIR = "data"        
RECONNECT_DELAY = 5      

logging.basicConfig(
    level=logging.INFO,                           
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),                
        logging.FileHandler("recorder.log"),      
    ],
)
log = logging.getLogger(__name__)  

last_u: dict[str, int] = {}

def check_gap(stream: str, msg: dict) -> None:
    if msg.get("e") != "depthUpdate":   
        return
    U, u = msg["U"], msg["u"]           # unpack first and last update
    prev = last_u.get(stream)          
    if prev is not None and U != prev + 1:  # verify if a number is skipped
        log.warning("GAP %s: expected U=%d got U=%d (missed %d updates)", stream, prev + 1, U, U - prev - 1)
    last_u[stream] = u  


def build_stream_path(symbols: list[str]) -> str:
    parts = []
    for s in symbols:
        parts.append(f"{s}@depth@100ms")  # L2 diff stream at 100 ms granularity
        parts.append(f"{s}@trade")        # individual trade stream (every matched order)
    return "/".join(parts)  


def today_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")  


def open_outfile(venue: str, date_str: str):
    os.makedirs(DATA_DIR, exist_ok=True)  
    path = f"{DATA_DIR}/{venue}_{date_str}.jsonl.gz"
    return gzip.open(path, "at")  


async def record_venue(venue: str) -> None:
    url = ENDPOINTS[venue] + build_stream_path(SYMBOLS[venue])  # full WebSocket URL with all streams

    while True:  # reconnect loop 
        try:
            async with websockets.connect(
                url,
                ping_interval=20,   # send a WebSocket ping every 20 s to keep the connection alive
                ping_timeout=30,    # if no pong arrives within 30 s, treat the connection as dead
            ) as ws:
                log.info("connected %s", venue)
                date_str = today_str()
                fh = open_outfile(venue, date_str) 

                try:
                    async for raw in ws:  # iterate over incoming WebSocket frames 
                        now = time.time()   

                        new_date = today_str()       
                        if new_date != date_str:    
                            fh.close()
                            date_str = new_date
                            fh = open_outfile(venue, date_str)

                        envelope = json.loads(raw)                       
                        stream_name = envelope.get("stream", "")        
                        data = envelope.get("data", envelope)           

                        check_gap(stream_name, data) 

                        record = {"t": now, "s": stream_name, "d": data} 
                        fh.write(json.dumps(record) + "\n")             

                except Exception as e:
                    log.error("stream error %s: %s", venue, e)  
                finally:
                    fh.close()  

        except Exception as e:
            log.error("connection error %s: %s", venue, e)  

        log.info("reconnecting %s in %ds …", venue, RECONNECT_DELAY)
        await asyncio.sleep(RECONNECT_DELAY)  


async def main() -> None:
    await asyncio.gather(*(record_venue(v) for v in SYMBOLS))  


if __name__ == "__main__":
    asyncio.run(main())  