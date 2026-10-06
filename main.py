import os
import json
import time
import math
import queue
import logging
import threading
import collections
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import requests
import websocket


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

log = logging.getLogger("xau-bubble-bot")


# ============================================================
# CONFIGURAZIONE
# ============================================================

FINNHUB_TOKEN = os.getenv("FINNHUB_TOKEN", "").strip()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

PORT = int(os.getenv("PORT", "10000"))

SYMBOL = os.getenv(
    "FINNHUB_SYMBOL",
    "OANDA:XAU_USD",
).strip()

# Bubble detector
PIVOT_LOOKBACK = int(os.getenv("PIVOT_LOOKBACK", "10"))
MIN_HISTORY = int(os.getenv("MIN_HISTORY", "20"))
HISTORY_SIZE = int(os.getenv("HISTORY_SIZE", "150"))

ZSCORE_THRESHOLD = float(
    os.getenv("ZSCORE_THRESHOLD", "2.0")
)

IMBALANCE_THRESHOLD = float(
    os.getenv("IMBALANCE_THRESHOLD", "3.0")
)

# Impedisce raffiche dello stesso segnale
ALERT_COOLDOWN_SECONDS = int(
    os.getenv("ALERT_COOLDOWN_SECONDS", "300")
)

# Se Finnhub restituisce volume=0 utilizziamo tick-volume.
USE_TICK_VOLUME_FALLBACK = (
    os.getenv("USE_TICK_VOLUME_FALLBACK", "true")
    .lower()
    in ("1", "true", "yes", "on")
)


TIMEFRAMES = {
    "M1": 60,
    "M3": 180,
    "M5": 300,
    "M15": 900,
    "M30": 1800,
    "H1": 3600,
    "H4": 14400,
    "D1": 86400,
}


# ============================================================
# VALIDAZIONE
# ============================================================

def validate_config():
    missing = []

    if not FINNHUB_TOKEN:
        missing.append("FINNHUB_TOKEN")

    if not TELEGRAM_TOKEN:
        missing.append("TELEGRAM_BOT_TOKEN")

    if not TELEGRAM_CHAT_ID:
        missing.append("TELEGRAM_CHAT_ID")

    if missing:
        raise RuntimeError(
            "Variabili Render mancanti: "
            + ", ".join(missing)
        )

    if PIVOT_LOOKBACK < 2:
        raise RuntimeError(
            "PIVOT_LOOKBACK deve essere >= 2"
        )

    if MIN_HISTORY < 5:
        raise RuntimeError(
            "MIN_HISTORY deve essere >= 5"
        )

    if ZSCORE_THRESHOLD <= 0:
        raise RuntimeError(
            "ZSCORE_THRESHOLD deve essere > 0"
        )

    if IMBALANCE_THRESHOLD <= 1:
        raise RuntimeError(
            "IMBALANCE_THRESHOLD deve essere > 1"
        )


# ============================================================
# DATI CANDELA
# ============================================================

class Candle:
    def __init__(self):
        self.period = None

        self.open = None
        self.high = None
        self.low = None
        self.close = None

        self.buy_volume = 0.0
        self.sell_volume = 0.0

        self.tick_count = 0
        self.real_volume_seen = False

        self.alerted = False

    @property
    def total_volume(self):
        return self.buy_volume + self.sell_volume

    @property
    def delta(self):
        return self.buy_volume - self.sell_volume


candles = {
    tf: Candle()
    for tf in TIMEFRAMES
}


volume_history = {
    tf: collections.deque(maxlen=HISTORY_SIZE)
    for tf in TIMEFRAMES
}

high_history = {
    tf: collections.deque(maxlen=PIVOT_LOOKBACK)
    for tf in TIMEFRAMES
}

low_history = {
    tf: collections.deque(maxlen=PIVOT_LOOKBACK)
    for tf in TIMEFRAMES
}


# ============================================================
# STATO
# ============================================================

state_lock = threading.RLock()

telegram_queue = queue.Queue(maxsize=100)

last_price = None
last_tick_time = None
last_connection_time = None

last_alert = {}

shutdown_event = threading.Event()


# ============================================================
# TELEGRAM
# ============================================================

def telegram_url():
    return (
        "https://api.telegram.org/bot"
        f"{TELEGRAM_TOKEN}/sendMessage"
    )


def enqueue_telegram(message):
    """
    Telegram viene usato SOLO per bubble confermate.
    Nessun messaggio ONLINE.
    Nessun messaggio di reconnect.
    """

    try:
        telegram_queue.put_nowait(message)

    except queue.Full:
        log.error(
            "Coda Telegram piena: alert scartato."
        )


def telegram_worker():
    session = requests.Session()

    while not shutdown_event.is_set():

        try:
            message = telegram_queue.get(timeout=1)

        except queue.Empty:
            continue

        try:
            payload = {
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
            }

            response = session.post(
                telegram_url(),
                json=payload,
                timeout=(5, 10),
            )

            if response.ok:
                log.info(
                    "Bubble inviata su Telegram."
                )

            else:
                log.error(
                    "Telegram HTTP %s: %s",
                    response.status_code,
                    response.text[:300],
                )

        except requests.RequestException as exc:
            log.error(
                "Errore rete Telegram: %s",
                exc,
            )

        except Exception:
            log.exception(
                "Errore inatteso Telegram."
            )

        finally:
            telegram_queue.task_done()


# ============================================================
# STATISTICA
# ============================================================

def volume_zscore(tf, current_volume):
    history = volume_history[tf]

    if len(history) < MIN_HISTORY:
        return None

    mean = sum(history) / len(history)

    variance = sum(
        (x - mean) ** 2
        for x in history
    ) / len(history)

    std = math.sqrt(variance)

    if std <= 0:
        return 0.0

    return (
        current_volume - mean
    ) / std


def imbalance_ratio(candle):
    buy = candle.buy_volume
    sell = candle.sell_volume

    if buy <= 0 and sell <= 0:
        return 0.0

    if buy <= 0 or sell <= 0:
        return float("inf")

    return max(
        buy / sell,
        sell / buy,
    )


def delta_percent(candle):
    total = candle.total_volume

    if total <= 0:
        return 0.0

    return (
        candle.delta / total
    ) * 100.0


# ============================================================
# COOLDOWN ALERT
# ============================================================

def alert_allowed(tf, direction):
    key = f"{tf}:{direction}"

    now = time.time()

    previous = last_alert.get(key, 0)

    if (
        now - previous
        < ALERT_COOLDOWN_SECONDS
    ):
        return False

    last_alert[key] = now

    return True


# ============================================================
# BUBBLE SIZE / CONFIDENCE
# ============================================================

def bubble_size(zscore):
    if zscore >= 4.0:
        return "EXTREME"

    if zscore >= 3.0:
        return "LARGE"

    return "MEDIUM"


def confidence_score(
    zscore,
    imbalance,
    sweep_strength,
):
    z_component = min(
        max(zscore / 4.0, 0),
        1,
    )

    if math.isinf(imbalance):
        imbalance_component = 1.0
    else:
        imbalance_component = min(
            imbalance / 5.0,
            1,
        )

    sweep_component = min(
        max(sweep_strength, 0),
        1,
    )

    score = (
        z_component * 0.45
        + imbalance_component * 0.35
        + sweep_component * 0.20
    )

    return max(
        0,
        min(100, round(score * 100)),
    )


# ============================================================
# FORMATTA ALERT
# ============================================================

def build_alert(
    tf,
    candle,
    direction,
    liquidity_level,
    zscore,
    imbalance,
    confidence,
    volume_mode,
):
    if direction == "LONG":
        icon = "🔴🫧"
        bubble_type = "BULLISH MANIPULATION BUBBLE"
        sweep = "SELL-SIDE LIQUIDITY SWEEP"

    else:
        icon = "🟢🫧"
        bubble_type = "BEARISH MANIPULATION BUBBLE"
        sweep = "BUY-SIDE LIQUIDITY SWEEP"

    ratio = (
        "∞"
        if math.isinf(imbalance)
        else f"{imbalance:.2f}x"
    )

    return (
        f"{icon} VOLUMETRIC BUBBLE DETECTED\n\n"
        f"XAU/USD | {tf}\n\n"

        f"{bubble_type}\n"
        f"{sweep}\n\n"

        f"Prezzo: {candle.close:.3f}\n"
        f"Livello liquidità: {liquidity_level:.3f}\n\n"

        f"VOLUMETRIC DATA\n"
        f"Buy Volume: {candle.buy_volume:.2f}\n"
        f"Sell Volume: {candle.sell_volume:.2f}\n"
        f"Delta: {candle.delta:+.2f}\n"
        f"Delta %: {delta_percent(candle):+.1f}%\n"
        f"Imbalance: {ratio}\n"
        f"Volume Z-Score: {zscore:.2f}\n\n"

        f"Bubble Size: {bubble_size(zscore)}\n"
        f"Bias: {direction}\n"
        f"Confidence: {confidence}/100\n\n"

        f"Volume source: {volume_mode}"
    )


# ============================================================
# CHIUSURA CANDELA
# ============================================================

def store_closed_candle(tf, candle):
    if candle.open is None:
        return

    volume_history[tf].append(
        candle.total_volume
    )

    high_history[tf].append(
        candle.high
    )

    low_history[tf].append(
        candle.low
    )


# ============================================================
# NUOVA CANDELA
# ============================================================

def reset_candle(
    candle,
    period,
    price,
    volume,
    buy_side,
    real_volume,
):
    candle.period = period

    candle.open = price
    candle.high = price
    candle.low = price
    candle.close = price

    candle.buy_volume = (
        volume if buy_side else 0.0
    )

    candle.sell_volume = (
        0.0 if buy_side else volume
    )

    candle.tick_count = 1

    candle.real_volume_seen = real_volume

    candle.alerted = False


# ============================================================
# DETECTOR BUBBLE
# ============================================================

def detect_bubble(tf, candle):
    if candle.alerted:
        return

    if (
        len(high_history[tf])
        < PIVOT_LOOKBACK
    ):
        return

    if (
        len(low_history[tf])
        < PIVOT_LOOKBACK
    ):
        return

    liquidity_high = max(
        high_history[tf]
    )

    liquidity_low = min(
        low_history[tf]
    )

    zscore = volume_zscore(
        tf,
        candle.total_volume,
    )

    if zscore is None:
        return

    if zscore < ZSCORE_THRESHOLD:
        return

    imbalance = imbalance_ratio(candle)

    if imbalance < IMBALANCE_THRESHOLD:
        return

    direction = None
    liquidity_level = None
    sweep_strength = 0.0

    candle_range = max(
        candle.high - candle.low,
        0.000001,
    )

    # --------------------------------------------------------
    # Sweep sopra i massimi + rientro
    # --------------------------------------------------------

    if (
        candle.high > liquidity_high
        and candle.close <= liquidity_high
    ):
        direction = "SHORT"

        liquidity_level = liquidity_high

        sweep_strength = min(
            (
                candle.high
                - liquidity_high
            )
            / candle_range,
            1.0,
        )

    # --------------------------------------------------------
    # Sweep sotto i minimi + rientro
    # --------------------------------------------------------

    elif (
        candle.low < liquidity_low
        and candle.close >= liquidity_low
    ):
        direction = "LONG"

        liquidity_level = liquidity_low

        sweep_strength = min(
            (
                liquidity_low
                - candle.low
            )
            / candle_range,
            1.0,
        )

    if direction is None:
        return

    if not alert_allowed(
        tf,
        direction,
    ):
        return

    confidence = confidence_score(
        zscore,
        imbalance,
        sweep_strength,
    )

    volume_mode = (
        "Finnhub volume"
        if candle.real_volume_seen
        else "tick-volume proxy"
    )

    candle.alerted = True

    message = build_alert(
        tf=tf,
        candle=candle,
        direction=direction,
        liquidity_level=liquidity_level,
        zscore=zscore,
        imbalance=imbalance,
        confidence=confidence,
        volume_mode=volume_mode,
    )

    log.info(
        "BUBBLE %s %s @ %.3f | Z=%.2f",
        tf,
        direction,
        candle.close,
        zscore,
    )

    enqueue_telegram(message)


# ============================================================
# MARKET FLOW
# ============================================================

def process_tick(
    price,
    raw_volume,
    timestamp_ms,
):
    global last_price
    global last_tick_time

    try:
        price = float(price)
        raw_volume = float(raw_volume or 0)
        timestamp_ms = int(timestamp_ms)

    except (TypeError, ValueError):
        return

    if price <= 0:
        return

    real_volume = raw_volume > 0

    if real_volume:
        volume = raw_volume

    elif USE_TICK_VOLUME_FALLBACK:
        volume = 1.0

    else:
        return

    timestamp_sec = (
        timestamp_ms / 1000.0
    )

    with state_lock:

        last_tick_time = time.time()

        if last_price is None:
            last_price = price
            return

        # Tick rule:
        # prezzo crescente = lato buy
        # prezzo decrescente = lato sell
        #
        # Questo è un proxy, non un vero
        # aggressor bid/ask Level-2.

        buy_side = price >= last_price

        last_price = price

        for tf, seconds in TIMEFRAMES.items():
            candle = candles[tf]

            period = int(
                timestamp_sec // seconds
            )

            if candle.period is None:
                reset_candle(
                    candle,
                    period,
                    price,
                    volume,
                    buy_side,
                    real_volume,
                )
                continue

            if period > candle.period:
                store_closed_candle(
                    tf,
                    candle,
                )

                reset_candle(
                    candle,
                    period,
                    price,
                    volume,
                    buy_side,
                    real_volume,
                )

                continue

            candle.close = price
            candle.tick_count += 1

            if price > candle.high:
                candle.high = price

            if price < candle.low:
                candle.low = price

            if buy_side:
                candle.buy_volume += volume
            else:
                candle.sell_volume += volume

            if real_volume:
                candle.real_volume_seen = True

            detect_bubble(
                tf,
                candle,
            )


# ============================================================
# FINNHUB CALLBACKS
# ============================================================

def on_open(ws):
    global last_connection_time

    last_connection_time = time.time()

    payload = {
        "type": "subscribe",
        "symbol": SYMBOL,
    }

    ws.send(
        json.dumps(payload)
    )

    # SOLO LOG. NIENTE TELEGRAM.
    log.info(
        "Finnhub connesso: %s",
        SYMBOL,
    )


def on_message(ws, message):
    try:
        payload = json.loads(message)

    except json.JSONDecodeError:
        log.warning(
            "Messaggio Finnhub non JSON."
        )
        return

    if payload.get("type") == "ping":
        return

    if payload.get("type") == "error":
        log.error(
            "Finnhub error: %s",
            payload,
        )
        return

    if payload.get("type") != "trade":
        return

    trades = payload.get(
        "data",
        [],
    )

    for trade in trades:
        if trade.get("s") != SYMBOL:
            continue

        process_tick(
            trade.get("p"),
            trade.get("v", 0),
            trade.get("t"),
        )


def on_error(ws, error):
    # SOLO LOG.
    log.error(
        "WebSocket: %s",
        error,
    )


def on_close(
    ws,
    status_code,
    message,
):
    # SOLO LOG.
    log.warning(
        "Finnhub disconnesso | code=%s | %s",
        status_code,
        message,
    )


# ============================================================
# WEBSOCKET LOOP
# ============================================================

def websocket_loop():
    delay = 2

    while not shutdown_event.is_set():

        try:
            ws = websocket.WebSocketApp(
                (
                    "wss://ws.finnhub.io"
                    f"?token={FINNHUB_TOKEN}"
                ),
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )

            ws.run_forever(
                ping_interval=30,
                ping_timeout=10,
            )

            # Se run_forever termina,
            # aspettiamo prima di riconnetterci.

        except KeyboardInterrupt:
            shutdown_event.set()
            break

        except Exception:
            log.exception(
                "Errore WebSocket."
            )

        if shutdown_event.is_set():
            break

        log.warning(
            "Riconnessione Finnhub tra %ss.",
            delay,
        )

        time.sleep(delay)

        # Exponential backoff fino a 60 sec.
        delay = min(
            delay * 2,
            60,
        )

        # Se la connessione precedente è durata
        # abbastanza, ripartiamo da 2 secondi.
        if (
            last_connection_time
            and time.time()
            - last_connection_time > 120
        ):
            delay = 2


# ============================================================
# HEALTH SERVER RENDER
# ============================================================

class HealthHandler(
    BaseHTTPRequestHandler
):
    def log_message(
        self,
        format,
        *args,
    ):
        return

    def _send_json(
        self,
        status,
        payload,
    ):
        body = json.dumps(
            payload
        ).encode("utf-8")

        self.send_response(status)

        self.send_header(
            "Content-Type",
            "application/json",
        )

        self.send_header(
            "Content-Length",
            str(len(body)),
        )

        self.end_headers()

        self.wfile.write(body)

    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()

    def do_GET(self):
        now = time.time()

        with state_lock:
            tick_age = (
                None
                if last_tick_time is None
                else round(
                    now - last_tick_time,
                    1,
                )
            )

        self._send_json(
            200,
            {
                "status": "online",
                "symbol": SYMBOL,
                "last_tick_age_seconds": tick_age,
                "telegram_queue": (
                    telegram_queue.qsize()
                ),
                "timeframes": list(
                    TIMEFRAMES.keys()
                ),
            },
        )


def health_server():
    server = ThreadingHTTPServer(
        ("0.0.0.0", PORT),
        HealthHandler,
    )

    log.info(
        "Health server 0.0.0.0:%s",
        PORT,
    )

    server.serve_forever()


# ============================================================
# MAIN
# ============================================================

def main():
    validate_config()

    log.info(
        "Avvio XAU/USD Volumetric Bubble Detector."
    )

    # NESSUN MESSAGGIO TELEGRAM DI AVVIO.

    threading.Thread(
        target=telegram_worker,
        name="telegram-worker",
        daemon=True,
    ).start()

    threading.Thread(
        target=health_server,
        name="health-server",
        daemon=True,
    ).start()

    # Il WebSocket resta nel thread principale.
    websocket_loop()


if __name__ == "__main__":
    main()
