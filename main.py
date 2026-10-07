# Closed-bar logic adapted from © LuxAlgo Volumetric Order Flow Structure.
# CC BY-NC-SA 4.0: https://creativecommons.org/licenses/by-nc-sa/4.0/
import os
import json
import time
import queue
import logging
import threading
import collections
import math
import copy
import hashlib
from datetime import datetime, time as daytime, timedelta, timezone
from zoneinfo import ZoneInfo
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
MULTI_TF_WINDOW_SECONDS = 0.05
SAME_BUBBLE_COOLDOWN_SECONDS = 300
MAX_HISTORY = 300
USE_TICK_VOLUME_FALLBACK = True

TIMEFRAMES = {
    "M1": 60, "M3": 180, "M5": 300, "M15": 900,
    "M30": 1800, "H1": 3600, "H4": 14400, "D1": 86400, "W1": 604800,
}

def candle_period(tf, timestamp):
    # Weekly buckets begin Monday 00:00 UTC, rather than Unix epoch Thursday.
    if tf == "H4":
        instant = datetime.fromtimestamp(timestamp, timezone.utc)
        ny = ZoneInfo("America/New_York")
        date = instant.astimezone(ny).date()
        anchor = datetime.combine(date, daytime(17), ny).astimezone(timezone.utc)
        if instant < anchor:
            anchor = datetime.combine(date - timedelta(days=1), daytime(17), ny).astimezone(timezone.utc)
        start = anchor.timestamp() + int((timestamp - anchor.timestamp()) // 14400) * 14400
        return int(start // 14400)
    offset = 3 * 86400 if tf == "W1" else 0
    return int((timestamp + offset) // TIMEFRAMES[tf])

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
    complete: bool = True

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
    event_id: str = ""
    feed_timestamp: float = 0.0

committed_structure = {}
confirmations = {}
history_status = {tf: "not_checked" for tf in TIMEFRAMES}
history_retry_after = 0

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
last_feed_timestamp = None
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

def enqueue_telegram(message, event_ids=None):
    try:
        telegram_queue.put_nowait({"text": message, "event_ids": event_ids or []})
    except queue.Full:
        log.error("Coda Telegram piena: alert scartato.")

def telegram_worker():
    session = requests.Session()
    while not shutdown_event.is_set():
        try:
            job = telegram_queue.get(timeout=1)
            message = job["text"] if isinstance(job, dict) else job
            event_ids = job.get("event_ids", []) if isinstance(job, dict) else []
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
                            "event_ids": event_ids,
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
    # Pine uses synthetic row weights from the pivot candle body, not tick volume.
    if candle.high <= candle.low:
        return candle.close
    step = (candle.high - candle.low) / PROFILE_ROWS
    body_high, body_low = max(candle.open, candle.close), min(candle.open, candle.close)
    weights = []
    for i in range(PROFILE_ROWS):
        top = candle.high - i * step
        bottom = top - step
        weight = int(max(2, 12 - abs(i - PROFILE_ROWS / 2.0) * 1.5))
        if body_low <= top <= body_high or body_low <= bottom <= body_high:
            weight += 5
        weights.append(weight)
    row = max(range(PROFILE_ROWS), key=lambda i: weights[i])
    return candle.high - step * (row + 0.5)

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
    candles = list(history[tf])[-(MAX_VOLUME_LOOKBACK - 1):]
    max_vol = max((c.volume for c in candles), default=0.0)
    return max(max_vol, current_volume, 1.0)

def is_pivot_high(candles, index):
    center = candles[index].high
    return all(
        candles[i].high <= center if i < index else candles[i].high < center
        for i in range(index - PIVOT_LENGTH, index + PIVOT_LENGTH + 1)
        if i != index
    )

def is_pivot_low(candles, index):
    center = candles[index].low
    return all(
        candles[i].low >= center if i < index else candles[i].low > center
        for i in range(index - PIVOT_LENGTH, index + PIVOT_LENGTH + 1)
        if i != index
    )

def zones_overlap(a, b):
    return a.low < b.high and a.high > b.low

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

def detect_new_pivots(tf, current=None):
    candles = list(history[tf]) + ([current] if current is not None else [])
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

def detect_structure_break(tf, candle, previous_high=None, previous_low=None):
    # Pine source: ta.crossover(close, lastPh) / ta.crossunder(close, lastPl).
    if len(history[tf]) == 0:
        return
    prev_close = history[tf][-1].close

    ph = last_pivot_high[tf]
    if ph is not None and previous_high is not None and prev_close <= previous_high and candle.close > ph:
        pivot_candle = last_pivot_high_candle[tf]
        if pivot_candle is not None:
            add_order_block(tf, pivot_candle, bullish=True)
        trend_state[tf] = 1
        last_pivot_high[tf] = None
        last_pivot_high_candle[tf] = None

    pl = last_pivot_low[tf]
    if pl is not None and previous_low is not None and prev_close >= previous_low and candle.close < pl:
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
    event = confirmations.get(tf)
    if event is not None and ((event.direction == "LONG" and candle.close < event.zone_low)
                              or (event.direction == "SHORT" and candle.close > event.zone_high)):
        confirmations.pop(tf, None)

def alert_allowed(tf, direction, block_id):
    current = current_candles[tf]
    period = current.period if current is not None else candle_period(tf, time.time())
    key = f"{tf}:{direction}:{period}:{block_id}"
    now = time.time()
    if key in last_alert:
        return False
    last_alert[key] = now
    # Keep identities for long candles (including weekly), bound memory by count.
    if len(last_alert) > 10000:
        for old_key in sorted(last_alert, key=last_alert.get)[:1000]:
            last_alert.pop(old_key, None)
    return True

def enqueue_bubble(event):
    if event.tf in {"M3", "M5"}:
        with state_lock:
            confirmations[event.tf] = event
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

        if detected and tf == "M1" and not all(
            name in confirmations and confirmations[name].direction == direction
            and time.time() < (int(confirmations[name].timestamp // TIMEFRAMES[name]) + 1) * TIMEFRAMES[name]
            for name in ("M3", "M5")):
            continue

        if detected:
            event = BubbleEvent(
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
                event_id=hashlib.sha256(f"{tf}:{direction}:{candle.period}:{block.block_id}".encode()).hexdigest()[:20],
                feed_timestamp=last_feed_timestamp or candle.timestamp,
            )
            if tf in {"M3", "M5"}:
                confirmations[tf] = event
            if alert_allowed(tf, direction, block.block_id):
                enqueue_bubble(event)

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
        f"{icon} {event.tf} — {event.direction} (in formazione)\n"
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

def filter_m1_confirmation(events, now=None):
    """Each higher-TF confirmation expires at its NEXT candle close."""
    now = time.time() if now is None else now
    with state_lock:
        for event in events:
            if event.tf in {"M3", "M5"}:
                confirmations[event.tf] = event
        for tf, event in list(confirmations.items()):
            expiry = (int(event.timestamp // TIMEFRAMES[tf]) + 1) * TIMEFRAMES[tf]
            if now >= expiry:
                confirmations.pop(tf, None)
        return [event for event in events if event.tf != "M1" or all(
            tf in confirmations and confirmations[tf].direction == event.direction
            and confirmations[tf].timestamp <= event.timestamp
            for tf in ("M3", "M5"))]

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
            original_count = len(pending)
            pending = filter_m1_confirmation(pending)
            if len(pending) < original_count:
                log.info("M1 scartato: mancano conferme M3/M5 concordi e ancora valide.")
            if not pending:
                first_time = None
                continue
            pending.sort(key=lambda e: tf_order.get(e.tf, 999))
            message = build_multi_tf_message(pending)
            enqueue_telegram(message, [event.event_id for event in pending])
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
                        "event_id": e.event_id, "feed_timestamp": e.feed_timestamp,
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

STRUCTURE_MAPS = (order_blocks, last_pivot_high, last_pivot_low,
                  last_pivot_high_candle, last_pivot_low_candle, trend_state)

def snapshot_structure(tf):
    return copy.deepcopy([mapping[tf] for mapping in STRUCTURE_MAPS])

def restore_committed_structure(tf):
    if tf in committed_structure:
        values = copy.deepcopy(committed_structure[tf])
        for mapping, value in zip(STRUCTURE_MAPS, values):
            mapping[tf] = value

def evaluate_structure(tf, candle):
    previous_high, previous_low = last_pivot_high[tf], last_pivot_low[tf]
    detect_new_pivots(tf, candle)
    invalidate_blocks(tf, candle)
    detect_structure_break(tf, candle, previous_high, previous_low)

def close_candle(tf, candle, notify=True):
    restore_committed_structure(tf)
    evaluate_structure(tf, candle)
    history[tf].append(candle)
    committed_structure[tf] = snapshot_structure(tf)

def process_tick(price, raw_volume, timestamp_ms):
    global last_price, last_tick_time, last_feed_timestamp
    try:
        price = float(price)
        raw_volume = float(raw_volume or 0)
        timestamp_ms = int(timestamp_ms)
    except (TypeError, ValueError):
        return
    if not math.isfinite(price) or not math.isfinite(raw_volume) or price <= 0 or timestamp_ms <= 0:
        return
    volume = raw_volume if raw_volume > 0 else (1.0 if USE_TICK_VOLUME_FALLBACK else 0.0)
    if volume <= 0:
        return
    timestamp = timestamp_ms / 1000.0

    with state_lock:
        last_tick_time = time.time()
        last_feed_timestamp = timestamp
        last_price = price
        for tf, seconds in TIMEFRAMES.items():
            period = candle_period(tf, timestamp)
            candle = current_candles[tf]
            if candle is None:
                current_candles[tf] = new_candle(period, timestamp, price, volume)
                current_candles[tf].complete = False
                continue
            if period > candle.period:
                if candle.complete:
                    close_candle(tf, candle)
                else:
                    log.info("Candela parziale scartata: %s", tf)
                current_candles[tf] = new_candle(period, timestamp, price, volume)
                if period > candle.period + 1:
                    history_status[tf] = "gap_unrecovered"
                    confirmations.clear()
                    current_candles[tf].complete = False
                continue
            if period < candle.period:
                continue
            candle.close = price
            candle.high = max(candle.high, price)
            candle.low = min(candle.low, price)
            candle.volume += volume
            candle.tick_count += 1
            profile_add(candle, price, volume)
        for tf, candle in current_candles.items():
            if candle is not None and candle.complete:
                if tf not in committed_structure:
                    committed_structure[tf] = snapshot_structure(tf)
                restore_committed_structure(tf)
                evaluate_structure(tf, candle)
        # Refresh higher-TF validity before evaluating M1 on the same price update.
        for tf in ("M3", "M5"):
            event = confirmations.get(tf)
            candle = current_candles[tf]
            if event is not None and (candle is None or not candle.complete or not any(
                b.active and b.block_id == event.block_id and (
                    b.bullish and candle.low < b.low and candle.close >= b.low
                    or not b.bullish and candle.high > b.high and candle.close <= b.high)
                for b in order_blocks[tf])):
                confirmations.pop(tf, None)
        for tf in [name for name in TIMEFRAMES if name != "M1"] + ["M1"]:
            candle = current_candles[tf]
            if candle is not None and candle.complete:
                detect_manipulation_bubble(tf, candle)

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
        provisional = {tf: snapshot_structure(tf) for tf in TIMEFRAMES}
        for tf in TIMEFRAMES:
            restore_committed_structure(tf)
        payload = {
            "version": 2,
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
        for tf, values in provisional.items():
            for mapping, value in zip(STRUCTURE_MAPS, values):
                mapping[tf] = value
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
                if tf == "H4" and payload.get("version", 1) < 2:
                    history[tf].clear()
                    order_blocks[tf] = []
                    history_status[tf] = "alignment_changed_requires_history"
                    continue
                history[tf].clear()
                for item in payload.get("history", {}).get(tf, [])[-MAX_HISTORY:]:
                    history[tf].append(deserialize_candle(item))
                order_blocks[tf] = [
                    OrderBlock(**b) for b in payload.get("order_blocks", {}).get(tf, [])
                ][-40:]
            last_alert.clear()
            last_alert.update({k: float(v) for k, v in payload.get("last_alert", {}).items()})
            saved_structure = payload.get("structure_state", {})
            if payload.get("version", 1) < 2:
                saved_structure.pop("H4", None)
            for tf in TIMEFRAMES:
                st = saved_structure.get(tf, {})
                last_pivot_high[tf] = st.get("last_pivot_high")
                last_pivot_low[tf] = st.get("last_pivot_low")
                trend_state[tf] = int(st.get("trend", 0))
                high_period = st.get("last_pivot_high_period")
                low_period = st.get("last_pivot_low_period")
                last_pivot_high_candle[tf] = next((x for x in history[tf] if x.period == high_period), None)
                last_pivot_low_candle[tf] = next((x for x in history[tf] if x.period == low_period), None)
            for tf in TIMEFRAMES:
                committed_structure[tf] = snapshot_structure(tf)
        log.info("Stato ripristinato da %s", STATE_FILE)
    except Exception as exc:
        log.warning("Impossibile ripristinare lo stato: %s", exc)

def persistence_worker():
    while not shutdown_event.wait(PERSIST_EVERY_SECONDS):
        save_state()

def parse_forex_candles(payload, seconds, cutoff):
    if not isinstance(payload, dict) or payload.get("s") != "ok":
        raise ValueError("no_candles")
    columns = [payload.get(key) for key in ("t", "o", "h", "l", "c", "v")]
    if not all(isinstance(col, list) for col in columns) or len({len(col) for col in columns}) != 1:
        raise ValueError("invalid_candle_columns")
    candles = []
    for stamp, op, hi, lo, cl, vol in zip(*columns):
        stamp = int(stamp)
        values = [float(x) for x in (op, hi, lo, cl, vol)]
        if not all(math.isfinite(x) for x in values) or not (0 < values[2] <= min(values[0], values[3]) <= max(values[0], values[3]) <= values[1]) or values[4] < 0:
            raise ValueError("invalid_ohlcv")
        if stamp + seconds <= cutoff:
            period = candle_period("W1", stamp) if seconds == 604800 else stamp // seconds
            candles.append(Candle(period, stamp, *values))
    if any(b.timestamp <= a.timestamp for a, b in zip(candles, candles[1:])):
        raise ValueError("non_chronological_candles")
    return candles

def aggregate_history(candles, source_seconds, target_seconds):
    buckets = {}
    for candle in candles:
        if target_seconds == 14400:
            instant = datetime.fromtimestamp(candle.timestamp, timezone.utc)
            ny = ZoneInfo("America/New_York")
            date = instant.astimezone(ny).date()
            anchor = datetime.combine(date, daytime(17), ny).astimezone(timezone.utc)
            if instant < anchor:
                anchor = datetime.combine(date - timedelta(days=1), daytime(17), ny).astimezone(timezone.utc)
            start = int(anchor.timestamp()) + int((candle.timestamp - anchor.timestamp()) // 14400) * 14400
            bucket = start
        else:
            bucket = int(candle.timestamp // target_seconds) * target_seconds
        buckets.setdefault(bucket, []).append(candle)
    result = []
    required = target_seconds // source_seconds
    for start, group in sorted(buckets.items()):
        period = int(start // target_seconds)
        if len(group) != required or [int(c.timestamp) for c in group] != list(range(start, start + target_seconds, source_seconds)):
            continue
        result.append(Candle(period, start, group[0].open, max(c.high for c in group),
                             min(c.low for c in group), group[-1].close, sum(c.volume for c in group)))
    return result

def recover_history():
    """Replay provider OHLCV silently; never invent candles for gaps."""
    global history_retry_after
    if time.time() < history_retry_after:
        return
    cutoff = int(time.time())
    mapping = {"M1": ("1", 60), "M5": ("5", 300), "M15": ("15", 900),
               "M30": ("30", 1800), "H1": ("60", 3600), "D1": ("D", 86400), "W1": ("W", 604800)}
    retrieved = {}
    try:
        for tf, (resolution, seconds) in mapping.items():
            multiplier = 4 if tf == "H1" else 3 if tf == "M1" else 1
            lookback = seconds * MAX_HISTORY * multiplier * 2
            response = requests.get("https://finnhub.io/api/v1/forex/candle",
                params={"symbol": SYMBOL, "resolution": resolution, "from": cutoff - lookback, "to": cutoff},
                headers={"X-Finnhub-Token": FINNHUB_TOKEN}, timeout=(5, 15))
            if response.status_code in {401, 403}:
                for name in TIMEFRAMES:
                    history_status[name] = f"history_http_{response.status_code}"
                history_retry_after = cutoff + 3600
                log.error("Storico forex non accessibile: HTTP %s. Recupero non confermato; nessun dato inventato.", response.status_code)
                return
            if response.status_code != 200:
                raise ValueError(f"history_http_{response.status_code}")
            retrieved[tf] = parse_forex_candles(response.json(), seconds, cutoff)
        retrieved["M3"] = aggregate_history(retrieved["M1"], 60, 180)
        retrieved["H4"] = aggregate_history(retrieved["H1"], 3600, 14400)
        with state_lock:
            confirmations.clear()
            for tf, candles in retrieved.items():
                if len(candles) < 200:
                    history_status[tf] = "insufficient_provider_history"
                    continue
                # A complete replay supersedes partial tick-built candles.
                committed_structure.pop(tf, None)
                history[tf].clear()
                order_blocks[tf] = []
                last_pivot_high[tf] = last_pivot_low[tf] = None
                last_pivot_high_candle[tf] = last_pivot_low_candle[tf] = None
                trend_state[tf] = 0
                for candle in candles[-MAX_HISTORY:]:
                    close_candle(tf, candle, notify=False)
                current_candles[tf] = None
                history_status[tf] = "recovered_provider_ohlcv"
                log.info("Storico recuperato senza alert: %s | %s candele", tf, len(history[tf]))
        save_state()
    except (requests.RequestException, ValueError, TypeError, KeyError):
        history_retry_after = cutoff + 300
        log.error("Recupero storico non riuscito; verifica continuita non completata.")
        for tf in TIMEFRAMES:
            if history_status[tf] == "not_checked":
                history_status[tf] = "history_unavailable"

def on_open(ws):
    global last_connection_time, ws_connected, subscription_verified
    ws_connected = True
    subscription_verified = False
    last_connection_time = time.time()
    with state_lock:
        confirmations.clear()
        for candle in current_candles.values():
            if candle is not None:
                candle.complete = False
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
            prior_tick = last_tick_time
            process_tick(trade.get("p"), trade.get("v", 0), trade.get("t"))
            if last_tick_time is not None:
                subscription_verified = True
                feed_error = None
                if prior_tick is None:
                    log.info("Primo prezzo oro ricevuto: %s | %s", SYMBOL, last_price)

def feed_monitor():
    while not shutdown_event.wait(60):
        snap = health_snapshot()
        log.info("Stato feed: %s | prezzo=%s | eta_tick=%s | sottoscrizione_verificata=%s",
                 snap["status"], snap["last_price"], snap["last_tick_age_seconds"], snap["subscription_verified"])

def on_error(ws, error):
    global feed_error
    code = getattr(error, "status_code", None)
    feed_error = "provider_rate_limited" if code == 429 else "websocket_error"
    log.error("Connessione Finnhub non disponibile: %s", feed_error)

def on_close(ws, status_code, message):
    global ws_connected
    ws_connected = False
    with state_lock:
        confirmations.clear()
        for candle in current_candles.values():
            if candle is not None:
                candle.complete = False
    log.warning("Finnhub disconnesso | code=%s | %s", status_code, message)

def websocket_loop():
    delay = 60
    while not shutdown_event.is_set():
        if not resolve_symbol():
            shutdown_event.wait(60)
            continue
        recover_history()
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
            delay = 60
        if feed_error == "provider_rejected_subscription":
            delay = 300
        log.warning("Riconnessione Finnhub tra %ss.", delay)
        shutdown_event.wait(delay)
        delay = min(delay * 2, 300)

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
        "m1_requires": ["M3", "M5"],
        "confirmation_validity": "until_next_M3_or_M5_candle_close",
        "history_status": dict(history_status),
        "parity_scope": "closed_bar_logic; upstream volume and session alignment require comparison",
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
    log.info("Timeframe attivi: %s", ", ".join(TIMEFRAMES))
    threading.Thread(target=telegram_worker, name="telegram-worker", daemon=True).start()
    threading.Thread(target=bubble_aggregator, name="bubble-aggregator", daemon=True).start()
    threading.Thread(target=persistence_worker, name="persistence-worker", daemon=True).start()
    threading.Thread(target=health_server, name="health-server", daemon=True).start()
    threading.Thread(target=feed_monitor, name="feed-monitor", daemon=True).start()
    try:
        websocket_loop()
    finally:
        save_state()

if __name__ == "__main__":
    main()
