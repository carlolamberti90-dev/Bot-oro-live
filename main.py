import os
import json
import time
import queue
import logging
import threading
import collections
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import requests
import websocket

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("xau-lux-bubble")

FINNHUB_TOKEN = os.getenv("FINNHUB_TOKEN", "").strip()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
SYMBOL = os.getenv("FINNHUB_SYMBOL", "OANDA:XAU_USD").strip()
PORT = int(os.getenv("PORT", "10000"))

# Parametri uguali allo screenshot LuxAlgo dell'utente
PIVOT_LENGTH = 3
VOLUME_LOOKBACK = 22
MAX_RECENT_BLOCKS = 4
BUBBLE_SENSITIVITY = 2.5
PROFILE_ROWS = 15
ATR_LENGTH = 14
HIDE_OVERLAPPING_BLOCKS = True
SHOW_MANIPULATION_BUBBLES = True

# Raggruppa eventi quasi contemporanei, senza sopprimere timeframe diversi.
MULTI_TF_WINDOW_SECONDS = 3.0
# Evita il duplicato della stessa zona sullo stesso timeframe.
SAME_BUBBLE_COOLDOWN_SECONDS = 300
MAX_HISTORY = 300
USE_TICK_VOLUME_FALLBACK = True

TIMEFRAMES = {
    "M1": 60, "M3": 180, "M5": 300, "M15": 900,
    "M30": 1800, "H1": 3600, "H4": 14400, "D1": 86400,
}

@dataclass
class Candle:
    period: int
    timestamp: float
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    tick_count: int = 0
    # (prezzo, volume) per ricostruire il profilo a 15 righe a candela chiusa.
    samples: list = field(default_factory=list)

@dataclass
class OrderBlock:
    block_id: str
    high: float
    low: float
    volume: float
    bullish: bool
    poc: float
    created_period: int
    active: bool = True

@dataclass
class BubbleEvent:
    tf: str
    direction: str
    price: float
    zone_high: float
    zone_low: float
    poc: float
    relative_volume: float
    volume: float
    timestamp: float
    block_id: str

current_candles = {tf: None for tf in TIMEFRAMES}
history = {tf: collections.deque(maxlen=MAX_HISTORY) for tf in TIMEFRAMES}
order_blocks = {tf: [] for tf in TIMEFRAMES}

state_lock = threading.RLock()
telegram_queue = queue.Queue(maxsize=200)
bubble_queue = queue.Queue(maxsize=200)
shutdown_event = threading.Event()

last_price = None
last_tick_time = None
last_connection_time = None
last_alert = {}

def validate_config():
    missing = []
    if not FINNHUB_TOKEN:
        missing.append("FINNHUB_TOKEN")
    if not TELEGRAM_TOKEN:
        missing.append("TELEGRAM_BOT_TOKEN")
    if not TELEGRAM_CHAT_ID:
        missing.append("TELEGRAM_CHAT_ID")
    if missing:
        raise RuntimeError("Variabili Render mancanti: " + ", ".join(missing))

def telegram_url():
    return f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

def enqueue_telegram(message):
    try:
        telegram_queue.put_nowait(message)
    except queue.Full:
        log.error("Coda Telegram piena: alert scartato.")

def telegram_worker():
    session = requests.Session()
    while not shutdown_event.is_set():
        try:
            message = telegram_queue.get(timeout=1)
        except queue.Empty:
            continue
        try:
            r = session.post(
                telegram_url(),
                json={"chat_id": TELEGRAM_CHAT_ID, "text": message},
                timeout=(5, 10),
            )
            if r.ok:
                log.info("Bubble inviata su Telegram.")
            else:
                log.error("Telegram HTTP %s: %s", r.status_code, r.text[:300])
        except requests.RequestException as exc:
            log.error("Errore rete Telegram: %s", exc)
        except Exception:
            log.exception("Errore inatteso Telegram.")
        finally:
            telegram_queue.task_done()

def calculate_poc(candle):
    if not candle.samples or candle.high <= candle.low:
        return candle.close
    width = (candle.high - candle.low) / PROFILE_ROWS
    rows = [0.0] * PROFILE_ROWS
    for price, volume in candle.samples:
        idx = int((price - candle.low) / width)
        idx = max(0, min(PROFILE_ROWS - 1, idx))
        rows[idx] += volume
    poc_row = max(range(PROFILE_ROWS), key=lambda i: rows[i])
    return candle.low + width * (poc_row + 0.5)

def calculate_atr(tf):
    candles = list(history[tf])
    if len(candles) < ATR_LENGTH + 1:
        return None
    recent = candles[-(ATR_LENGTH + 1):]
    trs = []
    for i in range(1, len(recent)):
        c, prev = recent[i], recent[i - 1]
        trs.append(max(c.high - c.low, abs(c.high - prev.close), abs(c.low - prev.close)))
    return sum(trs) / len(trs) if trs else None

def relative_volume(tf, current_volume):
    candles = list(history[tf])
    if len(candles) < VOLUME_LOOKBACK:
        return 1.0
    sample = candles[-VOLUME_LOOKBACK:]
    avg = sum(c.volume for c in sample) / len(sample)
    return current_volume / avg if avg > 0 else 1.0

def is_pivot_high(candles, index):
    center = candles[index].high
    return all(
        candles[i].high < center
        for i in range(index - PIVOT_LENGTH, index + PIVOT_LENGTH + 1)
        if i != index
    )

def is_pivot_low(candles, index):
    center = candles[index].low
    return all(
        candles[i].low > center
        for i in range(index - PIVOT_LENGTH, index + PIVOT_LENGTH + 1)
        if i != index
    )

def zones_overlap(a, b):
    return not (a.high < b.low or a.low > b.high)

def add_order_block(tf, pivot_candle, bullish):
    atr = calculate_atr(tf)
    if atr is None:
        return
    candle_range = pivot_candle.high - pivot_candle.low
    if candle_range <= 0:
        return

    # ATR limita zone anormalmente spesse senza alterare il pivot.
    zone_range = min(candle_range, atr * 1.5)
    if bullish:
        low = pivot_candle.low
        high = min(pivot_candle.high, low + zone_range)
    else:
        high = pivot_candle.high
        low = max(pivot_candle.low, high - zone_range)

    block = OrderBlock(
        block_id=f"{tf}-{pivot_candle.period}-{'B' if bullish else 'S'}",
        high=high,
        low=low,
        volume=pivot_candle.volume,
        bullish=bullish,
        poc=calculate_poc(pivot_candle),
        created_period=pivot_candle.period,
    )

    blocks = order_blocks[tf]
    if HIDE_OVERLAPPING_BLOCKS:
        for old in blocks:
            if old.active and zones_overlap(old, block):
                if old.volume >= block.volume:
                    return
                old.active = False

    blocks.append(block)
    active = sorted((b for b in blocks if b.active), key=lambda b: b.created_period)
    while len(active) > MAX_RECENT_BLOCKS:
        active.pop(0).active = False

def detect_new_pivots(tf):
    candles = list(history[tf])
    required = PIVOT_LENGTH * 2 + 1
    if len(candles) < required:
        return
    index = len(candles) - PIVOT_LENGTH - 1
    if index < PIVOT_LENGTH:
        return
    pivot = candles[index]
    if is_pivot_high(candles, index):
        add_order_block(tf, pivot, bullish=False)
    if is_pivot_low(candles, index):
        add_order_block(tf, pivot, bullish=True)

def invalidate_blocks(tf, candle):
    for block in order_blocks[tf]:
        if not block.active:
            continue
        if block.bullish and candle.close < block.low:
            block.active = False
        elif (not block.bullish) and candle.close > block.high:
            block.active = False

def alert_allowed(tf, direction, block_id):
    key = f"{tf}:{direction}:{block_id}"
    now = time.time()
    if now - last_alert.get(key, 0) < SAME_BUBBLE_COOLDOWN_SECONDS:
        return False
    last_alert[key] = now
    return True

def enqueue_bubble(event):
    try:
        bubble_queue.put_nowait(event)
    except queue.Full:
        log.error("Coda bubble piena: evento scartato.")

def detect_manipulation_bubble(tf, candle):
    if not SHOW_MANIPULATION_BUBBLES:
        return
    rvol = relative_volume(tf, candle.volume)

    for block in [b for b in order_blocks[tf] if b.active]:
        # Demand: wick sotto la zona, chiusura di nuovo dentro/sopra => LONG verde.
        if block.bullish:
            detected = candle.low < block.low and candle.close >= block.low
            direction = "LONG"
        else:
            # Supply: wick sopra la zona, chiusura di nuovo dentro/sotto => SHORT rosso.
            detected = candle.high > block.high and candle.close <= block.high
            direction = "SHORT"

        if detected and alert_allowed(tf, direction, block.block_id):
            enqueue_bubble(BubbleEvent(
                tf=tf,
                direction=direction,
                price=candle.close,
                zone_high=block.high,
                zone_low=block.low,
                poc=block.poc,
                relative_volume=rvol,
                volume=candle.volume,
                timestamp=time.time(),
                block_id=block.block_id,
            ))

def bubble_strength(rvol):
    # Sensitivity regola la risposta della dimensione, non elimina il raid.
    scaled = rvol * BUBBLE_SENSITIVITY
    if scaled >= 7.5:
        return "EXTREME"
    if scaled >= 5.0:
        return "LARGE"
    if scaled >= 2.5:
        return "MEDIUM"
    return "SMALL"

def format_event(event):
    if event.direction == "SHORT":
        icon, raid = "🔴🫧", "Buy-side liquidity raid"
    else:
        icon, raid = "🟢🫧", "Sell-side liquidity raid"
    return (
        f"{icon} {event.tf} — {event.direction}\n"
        f"Prezzo: {event.price:.3f}\n"
        f"Zona: {event.zone_low:.3f} - {event.zone_high:.3f}\n"
        f"POC: {event.poc:.3f}\n"
        f"Relative Volume: {event.relative_volume:.2f}x\n"
        f"Volume: {event.volume:.2f}\n"
        f"Bubble: {bubble_strength(event.relative_volume)}\n"
        f"{raid}"
    )

def build_multi_tf_message(events):
    directions = {e.direction for e in events}
    if len(directions) == 1:
        direction = events[0].direction
        icon = "🔴🫧" if direction == "SHORT" else "🟢🫧"
        header = f"{icon} VOLUMETRIC MANIPULATION BUBBLE — {direction}"
    else:
        header = "🟡🫧 VOLUMETRIC BUBBLES — MIXED"

    parts = [header, "", "XAU/USD", ""]
    if len(events) > 1:
        parts += ["MULTI-TIMEFRAME: " + " • ".join(e.tf for e in events), ""]
    for event in events:
        parts += [format_event(event), ""]
    return "\n".join(parts).rstrip()

def bubble_aggregator():
    pending = []
    first_time = None
    tf_order = {tf: i for i, tf in enumerate(TIMEFRAMES)}
    while not shutdown_event.is_set():
        try:
            event = bubble_queue.get(timeout=0.25)
            pending.append(event)
            bubble_queue.task_done()
            if first_time is None:
                first_time = time.time()
        except queue.Empty:
            pass

        if pending and first_time is not None and time.time() - first_time >= MULTI_TF_WINDOW_SECONDS:
            pending.sort(key=lambda e: tf_order.get(e.tf, 999))
            enqueue_telegram(build_multi_tf_message(pending))
            log.info("Bubble confermate: %s", ", ".join(f"{e.tf}-{e.direction}" for e in pending))
            pending = []
            first_time = None

def new_candle(period, timestamp, price, volume):
    return Candle(
        period=period, timestamp=timestamp,
        open=price, high=price, low=price, close=price,
        volume=volume, tick_count=1, samples=[(price, volume)],
    )

def close_candle(tf, candle):
    # Il raid è confermato solo quando conosciamo il close della candela.
    detect_manipulation_bubble(tf, candle)
    history[tf].append(candle)
    invalidate_blocks(tf, candle)
    detect_new_pivots(tf)

def process_tick(price, raw_volume, timestamp_ms):
    global last_price, last_tick_time
    try:
        price = float(price)
        raw_volume = float(raw_volume or 0)
        timestamp_ms = int(timestamp_ms)
    except (TypeError, ValueError):
        return
    if price <= 0:
        return

    volume = raw_volume if raw_volume > 0 else (1.0 if USE_TICK_VOLUME_FALLBACK else 0.0)
    if volume <= 0:
        return
    timestamp = timestamp_ms / 1000.0

    with state_lock:
        last_tick_time = time.time()
        last_price = price
        for tf, seconds in TIMEFRAMES.items():
            period = int(timestamp // seconds)
            candle = current_candles[tf]
            if candle is None:
                current_candles[tf] = new_candle(period, timestamp, price, volume)
                continue
            if period > candle.period:
                close_candle(tf, candle)
                current_candles[tf] = new_candle(period, timestamp, price, volume)
                continue
            # Ignora eventuali trade fuori ordine appartenenti a periodi già chiusi.
            if period < candle.period:
                continue
            candle.close = price
            candle.high = max(candle.high, price)
            candle.low = min(candle.low, price)
            candle.volume += volume
            candle.tick_count += 1
            candle.samples.append((price, volume))

def on_open(ws):
    global last_connection_time
    last_connection_time = time.time()
    ws.send(json.dumps({"type": "subscribe", "symbol": SYMBOL}))
    log.info("Finnhub connesso: %s", SYMBOL)  # solo log, mai Telegram

def on_message(ws, message):
    try:
        payload = json.loads(message)
    except json.JSONDecodeError:
        return
    if payload.get("type") == "error":
        log.error("Finnhub error: %s", payload)
        return
    if payload.get("type") != "trade":
        return
    for trade in payload.get("data", []):
        if trade.get("s") == SYMBOL:
            process_tick(trade.get("p"), trade.get("v", 0), trade.get("t"))

def on_error(ws, error):
    log.error("WebSocket: %s", error)

def on_close(ws, status_code, message):
    log.warning("Finnhub disconnesso | code=%s | %s", status_code, message)

def websocket_loop():
    delay = 2
    while not shutdown_event.is_set():
        opened_at = None
        try:
            ws = websocket.WebSocketApp(
                f"wss://ws.finnhub.io?token={FINNHUB_TOKEN}",
                on_open=on_open, on_message=on_message,
                on_error=on_error, on_close=on_close,
            )
            opened_at = time.time()
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except KeyboardInterrupt:
            shutdown_event.set()
            break
        except Exception:
            log.exception("Errore WebSocket.")

        if shutdown_event.is_set():
            break
        # Se la sessione è stata stabile, riparte dal backoff minimo.
        if opened_at and time.time() - opened_at > 120:
            delay = 2
        log.warning("Riconnessione Finnhub tra %ss.", delay)
        time.sleep(delay)
        delay = min(delay * 2, 60)

class HealthHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        return

    def _send_json(self, status, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()

    def do_GET(self):
        now = time.time()
        with state_lock:
            tick_age = None if last_tick_time is None else round(now - last_tick_time, 1)
            active = {tf: sum(1 for b in order_blocks[tf] if b.active) for tf in TIMEFRAMES}
            closed = {tf: len(history[tf]) for tf in TIMEFRAMES}
        self._send_json(200, {
            "status": "online",
            "engine": "Lux-style Volumetric Manipulation Bubble Detector",
            "symbol": SYMBOL,
            "last_tick_age_seconds": tick_age,
            "pivot_length": PIVOT_LENGTH,
            "volume_lookback": VOLUME_LOOKBACK,
            "max_recent_blocks": MAX_RECENT_BLOCKS,
            "bubble_sensitivity": BUBBLE_SENSITIVITY,
            "profile_rows": PROFILE_ROWS,
            "active_blocks": active,
            "closed_candles": closed,
            "timeframes": list(TIMEFRAMES.keys()),
        })

def health_server():
    server = ThreadingHTTPServer(("0.0.0.0", PORT), HealthHandler)
    log.info("Health server 0.0.0.0:%s", PORT)
    server.serve_forever()

def main():
    validate_config()
    log.info(
        "Avvio XAU/USD bubble detector | Pivot=%s Lookback=%s Blocks=%s Sensitivity=%s",
        PIVOT_LENGTH, VOLUME_LOOKBACK, MAX_RECENT_BLOCKS, BUBBLE_SENSITIVITY,
    )
    # Telegram resta completamente silenzioso all'avvio/reconnect.
    threading.Thread(target=telegram_worker, name="telegram-worker", daemon=True).start()
    threading.Thread(target=bubble_aggregator, name="bubble-aggregator", daemon=True).start()
    threading.Thread(target=health_server, name="health-server", daemon=True).start()
    websocket_loop()

if __name__ == "__main__":
    main()
