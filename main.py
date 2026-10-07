import os
import json
import time
import queue
import logging
import threading
import collections
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import requests
import websocket

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("xau-lux-bubble")

FINNHUB_TOKEN = os.getenv("FINNHUB_TOKEN", "").strip()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
SYMBOL = os.getenv("FINNHUB_SYMBOL", "OANDA:XAU_USD").strip()
PORT = int(os.getenv("PORT", "10000"))
STATE_FILE = Path(os.getenv("STATE_FILE", "bot_state.json"))
STALE_AFTER_SECONDS = int(os.getenv("STALE_AFTER_SECONDS", "180"))
PERSIST_EVERY_SECONDS = int(os.getenv("PERSIST_EVERY_SECONDS", "60"))

PIVOT_LENGTH = 3
VOLUME_LOOKBACK = 22  # user setting; LuxAlgo source default is 20
MAX_RECENT_BLOCKS = 4  # user setting; LuxAlgo source default is 5
BUBBLE_SENSITIVITY = 2.5  # user setting; LuxAlgo source default is 1.0
PROFILE_ROWS = 15
ATR_LENGTH = 14
HIDE_OVERLAPPING_BLOCKS = True
SHOW_MANIPULATION_BUBBLES = True
MAX_VOLUME_LOOKBACK = 200
MULTI_TF_WINDOW_SECONDS = 3.0
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
    # Quantized price-volume map. Bounds memory versus storing every tick forever.
    profile: dict = field(default_factory=dict)

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
    bubble_scale: float
    volume: float
    timestamp: float
    block_id: str

current_candles = {tf: None for tf in TIMEFRAMES}
history = {tf: collections.deque(maxlen=MAX_HISTORY) for tf in TIMEFRAMES}
order_blocks = {tf: [] for tf in TIMEFRAMES}
last_pivot_high = {tf: None for tf in TIMEFRAMES}
last_pivot_low = {tf: None for tf in TIMEFRAMES}
last_pivot_high_candle = {tf: None for tf in TIMEFRAMES}
last_pivot_low_candle = {tf: None for tf in TIMEFRAMES}
trend_state = {tf: 0 for tf in TIMEFRAMES}

state_lock = threading.RLock()
telegram_queue = queue.Queue(maxsize=200)
bubble_queue = queue.Queue(maxsize=200)
shutdown_event = threading.Event()

last_price = None
last_tick_time = None
last_connection_time = None
last_alert_time = None
last_alert = {}
ws_connected = False
feed_error = None
subscription_verified = False

def resolve_symbol():
    """Validate against the provider's catalog; never substitute another broker."""
    global SYMBOL, feed_error
    exchange, _, instrument = SYMBOL.partition(":")
    try:
        response = requests.get(
            "https://finnhub.io/api/v1/forex/symbol",
            params={"exchange": exchange.lower()},
            headers={"X-Finnhub-Token": FINNHUB_TOKEN}, timeout=(5, 15),
        )
        if response.status_code != 200:
            feed_error = f"symbol_catalog_http_{response.status_code}"
            log.error("Catalogo forex non accessibile: HTTP %s", response.status_code)
            return False
        catalog = response.json()
        if not isinstance(catalog, list):
            feed_error = "invalid_symbol_catalog"
            return False
        normalized = instrument.replace("_", "").replace("/", "").upper()
        matches = [row["symbol"] for row in catalog if isinstance(row, dict)
                   and str(row.get("symbol", "")).partition(":")[0].upper() == exchange.upper()
                   and str(row.get("symbol", "")).partition(":")[2].replace("_", "").replace("/", "").upper() == normalized]
        if len(matches) != 1:
            feed_error = "requested_gold_symbol_not_available"
            log.error("Simbolo oro richiesto non disponibile nel catalogo %s", exchange)
            return False
        SYMBOL = matches[0]
        feed_error = None
        log.info("Simbolo verificato nel catalogo: %s", SYMBOL)
        return True
    except (requests.RequestException, ValueError):
        feed_error = "symbol_catalog_unavailable"
        log.error("Verifica catalogo forex non riuscita")
        return False

AUDIT_ENABLED = os.getenv("AUDIT_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
AUDIT_FILE = Path(os.getenv("AUDIT_FILE", "bubble_audit.jsonl"))
AUDIT_MAX_BYTES = int(os.getenv("AUDIT_MAX_BYTES", str(5 * 1024 * 1024)))
audit_lock = threading.Lock()

def audit_write(record):
    if not AUDIT_ENABLED:
        return
    try:
        line = json.dumps(record, separators=(",", ":"), ensure_ascii=False) + "\n"
        with audit_lock:
            if AUDIT_FILE.exists() and AUDIT_FILE.stat().st_size >= AUDIT_MAX_BYTES:
                rotated = AUDIT_FILE.with_suffix(AUDIT_FILE.suffix + ".1")
                try:
                    if rotated.exists():
                        rotated.unlink()
                    AUDIT_FILE.replace(rotated)
                except OSError:
                    pass
            with AUDIT_FILE.open("a", encoding="utf-8") as fh:
                fh.write(line)
    except Exception as exc:
        log.warning("Audit bubble non disponibile: %s", exc)

def validate_config():
    missing = []
    if not FINNHUB_TOKEN:
        missing.append("FINNHUB_TOKEN")
    if not TELEGRAM_TOKEN:
        missing.append("TELEGRAM_BOT_TOKEN")
    if not TELEGRAM_CHAT_ID:
        missing.append("TELEGRAM_CHAT_ID")
    if missing:
        raise RuntimeError("Variabili ambiente mancanti: " + ", ".join(missing))

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
            for attempt in range(4):
                try:
                    r = session.post(
                        telegram_url(),
                        json={"chat_id": TELEGRAM_CHAT_ID, "text": message},
                        timeout=(5, 10),
                    )
                    if r.ok:
                        audit_write({
                            "type": "telegram_sent",
                            "sent_at": time.time(),
                            "telegram_status": r.status_code,
                        })
                        log.info("Bubble inviata su Telegram.")
                        break
                    retryable = r.status_code == 429 or r.status_code >= 500
                    audit_write({
                        "type": "telegram_error",
                        "error_at": time.time(),
                        "telegram_status": r.status_code,
                        "response": r.text[:300],
                    })
                    log.error("Telegram HTTP %s: %s", r.status_code, r.text[:300])
                    if not retryable:
                        break
                except requests.RequestException as exc:
                    audit_write({
                        "type": "telegram_network_error",
                        "error_at": time.time(),
                        "error": str(exc),
                    })
                    log.error("Errore rete Telegram: %s", exc)
                if attempt < 3:
                    time.sleep(2 ** attempt)
        except Exception:
            log.exception("Errore inatteso Telegram.")
        finally:
            telegram_queue.task_done()

def profile_add(candle, price, volume):
    # Quantize to mill price resolution for XAU/USD and cap keys if needed.
    key = round(float(price), 3)
    candle.profile[key] = candle.profile.get(key, 0.0) + volume
    if len(candle.profile) > 2000:
        # Merge least-volume keys in batches to keep long candles bounded.
        smallest = sorted(candle.profile.items(), key=lambda kv: kv[1])[:500]
        merged_volume = sum(v for _, v in smallest)
        for k, _ in smallest:
            candle.profile.pop(k, None)
        candle.profile[round(candle.close, 3)] = candle.profile.get(round(candle.close, 3), 0.0) + merged_volume

def calculate_poc(candle):
    if not candle.profile or candle.high <= candle.low:
        return candle.close
    width = (candle.high - candle.low) / PROFILE_ROWS
    if width <= 0:
        return candle.close
    rows = [0.0] * PROFILE_ROWS
    for price, volume in candle.profile.items():
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

def max_volume_lookback(tf, current_volume=0.0):
    candles = list(history[tf])[-MAX_VOLUME_LOOKBACK:]
    max_vol = max((c.volume for c in candles), default=0.0)
    return max(max_vol, current_volume, 1.0)

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

def prune_blocks(tf):
    blocks = order_blocks[tf]
    active = [b for b in blocks if b.active]
    inactive = [b for b in blocks if not b.active][-20:]
    order_blocks[tf] = inactive + active[-MAX_RECENT_BLOCKS:]

def add_order_block(tf, pivot_candle, bullish):
    # LuxAlgo structure uses the full pivot candle range for the block.
    # ATR is used only for structure-line visuals in the Pine source, not to clip OB height.
    if pivot_candle.high <= pivot_candle.low:
        return

    block = OrderBlock(
        block_id=f"{tf}-{pivot_candle.period}-{'B' if bullish else 'S'}",
        high=pivot_candle.high,
        low=pivot_candle.low,
        volume=pivot_candle.volume,
        bullish=bullish,
        poc=calculate_poc(pivot_candle),
        created_period=pivot_candle.period,
    )

    blocks = order_blocks[tf]
    if HIDE_OVERLAPPING_BLOCKS:
        overlapping = [b for b in blocks if b.active and zones_overlap(b, block)]
        if overlapping:
            # LuxAlgo: only draw the new block if its pivot volume is strictly larger
            # than every overlapping active block; then remove all overlaps.
            if any(block.volume <= old.volume for old in overlapping):
                return
            for old in overlapping:
                old.active = False

    blocks.append(block)
    active = sorted((b for b in blocks if b.active), key=lambda b: b.created_period)
    while len(active) > MAX_RECENT_BLOCKS:
        active.pop(0).active = False
    prune_blocks(tf)

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
        last_pivot_high[tf] = pivot.high
        last_pivot_high_candle[tf] = pivot
    if is_pivot_low(candles, index):
        last_pivot_low[tf] = pivot.low
        last_pivot_low_candle[tf] = pivot

def detect_structure_break(tf, candle):
    # Pine source: ta.crossover(close, lastPh) / ta.crossunder(close, lastPl).
    if len(history[tf]) == 0:
        return
    prev_close = history[tf][-1].close

    ph = last_pivot_high[tf]
    if ph is not None and prev_close <= ph < candle.close:
        pivot_candle = last_pivot_high_candle[tf]
        if pivot_candle is not None:
            add_order_block(tf, pivot_candle, bullish=True)
        trend_state[tf] = 1
        last_pivot_high[tf] = None
        last_pivot_high_candle[tf] = None

    pl = last_pivot_low[tf]
    if pl is not None and prev_close >= pl > candle.close:
        pivot_candle = last_pivot_low_candle[tf]
        if pivot_candle is not None:
            add_order_block(tf, pivot_candle, bullish=False)
        trend_state[tf] = -1
        last_pivot_low[tf] = None
        last_pivot_low_candle[tf] = None

def invalidate_blocks(tf, candle):
    for block in order_blocks[tf]:
        if not block.active:
            continue
        if block.bullish and candle.close < block.low:
            block.active = False
        elif (not block.bullish) and candle.close > block.high:
            block.active = False
    prune_blocks(tf)

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
    max_vol = max_volume_lookback(tf, candle.volume)
    bubble_scale = (candle.volume / max_vol) * BUBBLE_SENSITIVITY

    for block in [b for b in order_blocks[tf] if b.active]:
        # Exact Pine raid conditions.
        if block.bullish:
            detected = candle.low < block.low and candle.close >= block.low
            direction = "LONG"
        else:
            detected = candle.high > block.high and candle.close <= block.high
            direction = "SHORT"

        if detected:
            audit_write({
                "type": "candidate",
                "detected_at": time.time(),
                "tf": tf,
                "direction": direction,
                "price": candle.close,
                "zone_high": block.high,
                "zone_low": block.low,
                "poc": block.poc,
                "relative_volume": rvol,
                "bubble_scale": bubble_scale,
                "volume": candle.volume,
                "block_id": block.block_id,
                "candle_period": candle.period,
            })

        if detected and alert_allowed(tf, direction, block.block_id):
            enqueue_bubble(BubbleEvent(
                tf=tf,
                direction=direction,
                price=candle.close,
                zone_high=block.high,
                zone_low=block.low,
                poc=block.poc,
                relative_volume=rvol,
                bubble_scale=bubble_scale,
                volume=candle.volume,
                timestamp=time.time(),
                block_id=block.block_id,
            ))

def bubble_strength(scale):
    # Mirrors Pine bubble-size thresholds: >0.8 huge, >0.6 large,
    # >0.4 normal, >0.2 small, else tiny.
    if scale > 0.8:
        return "HUGE"
    if scale > 0.6:
        return "LARGE"
    if scale > 0.4:
        return "NORMAL"
    if scale > 0.2:
        return "SMALL"
    return "TINY"

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
        f"Bubble Scale: {event.bubble_scale:.2f}\n"
        f"Bubble: {bubble_strength(event.bubble_scale)}\n"
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
    global last_alert_time
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
            message = build_multi_tf_message(pending)
            enqueue_telegram(message)
            last_alert_time = time.time()
            audit_write({
                "type": "telegram_enqueued",
                "enqueued_at": last_alert_time,
                "events": [
                    {
                        "tf": e.tf, "direction": e.direction, "price": e.price,
                        "zone_high": e.zone_high, "zone_low": e.zone_low,
                        "poc": e.poc, "relative_volume": e.relative_volume,
                        "bubble_scale": e.bubble_scale,
                        "volume": e.volume, "block_id": e.block_id,
                    } for e in pending
                ],
            })
            log.info("Bubble confermate: %s", ", ".join(f"{e.tf}-{e.direction}" for e in pending))
            pending = []
            first_time = None

def new_candle(period, timestamp, price, volume):
    c = Candle(
        period=period, timestamp=timestamp,
        open=price, high=price, low=price, close=price,
        volume=volume, tick_count=1,
    )
    profile_add(c, price, volume)
    return c

def close_candle(tf, candle):
    # Existing blocks are updated first, matching Pine's execution order.
    detect_manipulation_bubble(tf, candle)
    invalidate_blocks(tf, candle)
    detect_structure_break(tf, candle)
    history[tf].append(candle)
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
            if period < candle.period:
                continue
            candle.close = price
            candle.high = max(candle.high, price)
            candle.low = min(candle.low, price)
            candle.volume += volume
            candle.tick_count += 1
            profile_add(candle, price, volume)

def serialize_candle(c):
    return {
        "period": c.period, "timestamp": c.timestamp, "open": c.open,
        "high": c.high, "low": c.low, "close": c.close, "volume": c.volume,
        "tick_count": c.tick_count, "profile": c.profile,
    }

def deserialize_candle(d):
    c = Candle(
        period=int(d["period"]), timestamp=float(d["timestamp"]),
        open=float(d["open"]), high=float(d["high"]), low=float(d["low"]),
        close=float(d["close"]), volume=float(d.get("volume", 0)),
        tick_count=int(d.get("tick_count", 0)),
    )
    c.profile = {float(k): float(v) for k, v in d.get("profile", {}).items()}
    return c

def save_state():
    tmp = STATE_FILE.with_suffix(STATE_FILE.suffix + ".tmp")
    with state_lock:
        payload = {
            "version": 1,
            "saved_at": time.time(),
            "history": {tf: [serialize_candle(c) for c in history[tf]] for tf in TIMEFRAMES},
            "order_blocks": {
                tf: [
                    {
                        "block_id": b.block_id, "high": b.high, "low": b.low,
                        "volume": b.volume, "bullish": b.bullish, "poc": b.poc,
                        "created_period": b.created_period, "active": b.active,
                    }
                    for b in order_blocks[tf]
                ]
                for tf in TIMEFRAMES
            },
            "last_alert": last_alert,
            "structure_state": {
                tf: {
                    "last_pivot_high": last_pivot_high[tf],
                    "last_pivot_low": last_pivot_low[tf],
                    "last_pivot_high_period": None if last_pivot_high_candle[tf] is None else last_pivot_high_candle[tf].period,
                    "last_pivot_low_period": None if last_pivot_low_candle[tf] is None else last_pivot_low_candle[tf].period,
                    "trend": trend_state[tf],
                } for tf in TIMEFRAMES
            },
        }
    try:
        tmp.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
        tmp.replace(STATE_FILE)
    except Exception as exc:
        log.warning("Persistenza stato non disponibile: %s", exc)

def load_state():
    if not STATE_FILE.exists():
        return
    try:
        payload = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        with state_lock:
            for tf in TIMEFRAMES:
                history[tf].clear()
                for item in payload.get("history", {}).get(tf, [])[-MAX_HISTORY:]:
                    history[tf].append(deserialize_candle(item))
                order_blocks[tf] = [
                    OrderBlock(**b) for b in payload.get("order_blocks", {}).get(tf, [])
                ][-40:]
            last_alert.clear()
            last_alert.update({k: float(v) for k, v in payload.get("last_alert", {}).items()})
            saved_structure = payload.get("structure_state", {})
            for tf in TIMEFRAMES:
                st = saved_structure.get(tf, {})
                last_pivot_high[tf] = st.get("last_pivot_high")
                last_pivot_low[tf] = st.get("last_pivot_low")
                trend_state[tf] = int(st.get("trend", 0))
                high_period = st.get("last_pivot_high_period")
                low_period = st.get("last_pivot_low_period")
                last_pivot_high_candle[tf] = next((x for x in history[tf] if x.period == high_period), None)
                last_pivot_low_candle[tf] = next((x for x in history[tf] if x.period == low_period), None)
        log.info("Stato ripristinato da %s", STATE_FILE)
    except Exception as exc:
        log.warning("Impossibile ripristinare lo stato: %s", exc)

def persistence_worker():
    while not shutdown_event.wait(PERSIST_EVERY_SECONDS):
        save_state()

def on_open(ws):
    global last_connection_time, ws_connected, subscription_verified
    ws_connected = True
    subscription_verified = False
    last_connection_time = time.time()
    ws.send(json.dumps({"type": "subscribe", "symbol": SYMBOL}))
    log.info("Socket Finnhub aperto; richiesta dati: %s (in attesa del primo prezzo)", SYMBOL)

def on_message(ws, message):
    global feed_error, subscription_verified
    try:
        payload = json.loads(message)
    except json.JSONDecodeError:
        return
    if payload.get("type") == "error":
        feed_error = "provider_rejected_subscription"
        subscription_verified = False
        log.error("Finnhub rifiuta la sottoscrizione: %s", payload.get("msg", "errore"))
        ws.close()
        return
    if payload.get("type") != "trade":
        return
    for trade in payload.get("data", []):
        if trade.get("s") == SYMBOL:
            process_tick(trade.get("p"), trade.get("v", 0), trade.get("t"))
            if last_tick_time is not None:
                subscription_verified = True
                feed_error = None

def on_error(ws, error):
    log.error("WebSocket: %s", error)

def on_close(ws, status_code, message):
    global ws_connected
    ws_connected = False
    log.warning("Finnhub disconnesso | code=%s | %s", status_code, message)

def websocket_loop():
    delay = 2
    while not shutdown_event.is_set():
        if not resolve_symbol():
            shutdown_event.wait(60)
            continue
        started = time.time()
        try:
            ws = websocket.WebSocketApp(
                f"wss://ws.finnhub.io?token={FINNHUB_TOKEN}",
                on_open=on_open, on_message=on_message,
                on_error=on_error, on_close=on_close,
            )
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except KeyboardInterrupt:
            shutdown_event.set()
            break
        except Exception:
            log.exception("Errore WebSocket.")
        if shutdown_event.is_set():
            break
        if time.time() - started > 120:
            delay = 2
        log.warning("Riconnessione Finnhub tra %ss.", delay)
        shutdown_event.wait(delay)
        delay = min(delay * 2, 60)

def health_snapshot():
    now = time.time()
    with state_lock:
        tick_age = None if last_tick_time is None else round(now - last_tick_time, 1)
        active = {tf: sum(1 for b in order_blocks[tf] if b.active) for tf in TIMEFRAMES}
        closed = {tf: len(history[tf]) for tf in TIMEFRAMES}
        ready = {tf: len(history[tf]) >= max(VOLUME_LOOKBACK, ATR_LENGTH + 1, PIVOT_LENGTH * 2 + 1) for tf in TIMEFRAMES}
    if feed_error:
        status = "feed_error"
    elif last_tick_time is None:
        status = "starting"
    elif tick_age is not None and tick_age > STALE_AFTER_SECONDS:
        status = "feed_stale"
    elif not ws_connected:
        status = "disconnected"
    else:
        status = "online"
    return {
        "status": status,
        "engine": "Lux-style Volumetric Manipulation Bubble Detector",
        "symbol": SYMBOL,
        "websocket_connected": ws_connected,
        "subscription_verified": subscription_verified,
        "feed_error": feed_error,
        "last_price": last_price,
        "last_tick_age_seconds": tick_age,
        "last_alert_age_seconds": None if last_alert_time is None else round(now - last_alert_time, 1),
        "pivot_length": PIVOT_LENGTH,
        "volume_lookback": VOLUME_LOOKBACK,
        "max_recent_blocks": MAX_RECENT_BLOCKS,
        "bubble_sensitivity": BUBBLE_SENSITIVITY,
        "profile_rows": PROFILE_ROWS,
        "max_volume_lookback": MAX_VOLUME_LOOKBACK,
        "active_blocks": active,
        "closed_candles": closed,
        "ready": ready,
        "timeframes": list(TIMEFRAMES.keys()),
        "audit_enabled": AUDIT_ENABLED,
        "audit_file": str(AUDIT_FILE) if AUDIT_ENABLED else None,
    }

class HealthHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        return

    def _send_json(self, status, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_HEAD(self):
        snap = health_snapshot()
        self.send_response(200)
        self.end_headers()

    def do_GET(self):
        snap = health_snapshot()
        path = self.path.split("?", 1)[0]
        if path == "/ready":
            self._send_json(200 if snap["status"] == "online" else 503, snap)
        elif path in {"/", "/health"}:
            self._send_json(200, snap)
        else:
            self._send_json(404, {"status": "not_found"})

def health_server():
    server = ThreadingHTTPServer(("0.0.0.0", PORT), HealthHandler)
    log.info("Health server 0.0.0.0:%s", PORT)
    server.serve_forever()

def main():
    validate_config()
    load_state()
    log.info(
        "Avvio XAU/USD bubble detector | Pivot=%s Lookback=%s Blocks=%s Sensitivity=%s",
        PIVOT_LENGTH, VOLUME_LOOKBACK, MAX_RECENT_BLOCKS, BUBBLE_SENSITIVITY,
    )
    threading.Thread(target=telegram_worker, name="telegram-worker", daemon=True).start()
    threading.Thread(target=bubble_aggregator, name="bubble-aggregator", daemon=True).start()
    threading.Thread(target=persistence_worker, name="persistence-worker", daemon=True).start()
    threading.Thread(target=health_server, name="health-server", daemon=True).start()
    try:
        websocket_loop()
    finally:
        save_state()

if __name__ == "__main__":
    main()
