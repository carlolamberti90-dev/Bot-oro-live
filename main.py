import os
import json
import time
import threading
import websocket
import requests
import collections
import queue
import re
import statistics
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


# ============================================================
# CONFIGURAZIONE
# ============================================================

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
CHAT_ID_RAW = os.environ.get("TELEGRAM_CHAT_ID")
FINNHUB_TOKEN = os.environ.get("FINNHUB_TOKEN")
RENDER_APP_NAME = os.environ.get("RENDER_EXTERNAL_HOSTNAME")

SYMBOL = "OANDA:XAU_USD"

# Parametri algoritmo
LUX_PIVOT_LOOKBACK = 10
LUX_ZSCORE_THRESHOLD = 2.0
LUX_DELTA_RATIO = 3.0

# Storico utilizzato per il calcolo dello Z-Score
VOLUME_HISTORY_LENGTH = 150
MIN_VOLUME_HISTORY = 20

# WebSocket
RECONNECT_SECONDS = 5
PING_INTERVAL = 25
PING_TIMEOUT = 10

# Render fornisce normalmente PORT automaticamente.
PORT = int(os.environ.get("PORT", "10000"))


# ============================================================
# CONFIGURAZIONE TELEGRAM
# ============================================================

def clean_chat_id(raw_id):
    if not raw_id:
        return None

    cleaned = str(raw_id).strip()

    # ID numerico Telegram, compresi gli ID negativi dei gruppi
    if re.fullmatch(r"-?\d+", cleaned):
        return cleaned

    # Username Telegram
    if not cleaned.startswith("@"):
        return f"@{cleaned}"

    return cleaned


CHAT_ID = clean_chat_id(CHAT_ID_RAW)


# ============================================================
# AVVISI CONFIGURAZIONE
# ============================================================

if not FINNHUB_TOKEN:
    print(
        "⚠️ ATTENZIONE: FINNHUB_TOKEN non configurato.",
        flush=True,
    )

if not TELEGRAM_TOKEN:
    print(
        "⚠️ ATTENZIONE: TELEGRAM_BOT_TOKEN non configurato.",
        flush=True,
    )

if not CHAT_ID:
    print(
        "⚠️ ATTENZIONE: TELEGRAM_CHAT_ID non configurato.",
        flush=True,
    )


# ============================================================
# STRUTTURE DATI
# ============================================================

class VolumetricCandle:
    def __init__(self):
        self.reset(None, None)

    def reset(self, period, price):
        self.open = price

        if price is None:
            self.high = float("-inf")
            self.low = float("inf")
        else:
            self.high = price
            self.low = price

        self.close = price

        self.buy_vol = 0.0
        self.sell_vol = 0.0

        self.buy_ticks = 0
        self.sell_ticks = 0

        self.current_period = period
        self.alerted = False


# ============================================================
# TIMEFRAME
# ============================================================

timeframes = {
    "M1": 60,
    "M3": 180,
    "M5": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "4h": 14400,
    "D1": 86400,
}


candles_data = {
    tf: VolumetricCandle()
    for tf in timeframes
}


# ============================================================
# STORICI
# ============================================================

volume_history = {
    tf: collections.deque(maxlen=VOLUME_HISTORY_LENGTH)
    for tf in timeframes
}

high_history = {
    tf: collections.deque(maxlen=LUX_PIVOT_LOOKBACK)
    for tf in timeframes
}

low_history = {
    tf: collections.deque(maxlen=LUX_PIVOT_LOOKBACK)
    for tf in timeframes
}


# ============================================================
# THREAD / STATO
# ============================================================

last_processed_price = None

state_lock = threading.RLock()

telegram_queue = queue.Queue()


# ============================================================
# TELEGRAM
# ============================================================

def telegram_api_url():
    return (
        f"https://api.telegram.org/bot"
        f"{TELEGRAM_TOKEN}/sendMessage"
    )


def send_telegram_now(message):
    """
    Invia immediatamente un messaggio Telegram.
    """

    if not TELEGRAM_TOKEN or not CHAT_ID:
        print(
            "⚠️ Telegram non configurato. Alert non inviato.",
            flush=True,
        )
        return False

    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }

    try:
        response = requests.post(
            telegram_api_url(),
            json=payload,
            timeout=10,
        )

        if response.ok:
            print(
                "⚡ [Telegram] Notifica consegnata.",
                flush=True,
            )
            return True

        print(
            f"❌ [Telegram Error] HTTP "
            f"{response.status_code}: {response.text}",
            flush=True,
        )

        # Fallback senza Markdown
        if response.status_code == 400:
            payload.pop("parse_mode", None)

            retry = requests.post(
                telegram_api_url(),
                json=payload,
                timeout=10,
            )

            if retry.ok:
                print(
                    "⚡ [Telegram] Notifica consegnata "
                    "(fallback plain text).",
                    flush=True,
                )
                return True

            print(
                f"❌ [Telegram Retry] HTTP "
                f"{retry.status_code}: {retry.text}",
                flush=True,
            )

    except requests.RequestException as exc:
        print(
            f"❌ [Telegram Exception] {exc}",
            flush=True,
        )

    return False


def telegram_worker():
    """
    Worker separato per Telegram.
    Evita che le richieste HTTP blocchino il WebSocket.
    """

    while True:
        message = telegram_queue.get()

        try:
            send_telegram_now(message)

        except Exception as exc:
            print(
                f"❌ [Telegram Worker] {exc}",
                flush=True,
            )

        finally:
            telegram_queue.task_done()


def send_telegram_alert_instant(message):
    telegram_queue.put(message)


# ============================================================
# CALCOLO Z-SCORE VOLUME
# ============================================================

def calculate_volume_metrics(tf, total_vol):
    history = volume_history[tf]

    if len(history) < MIN_VOLUME_HISTORY:
        return 0.0, False

    mean = statistics.fmean(history)

    std_dev = statistics.pstdev(history)

    if std_dev <= 0:
        return 0.0, False

    z_score = (
        (total_vol - mean)
        / std_dev
    )

    return (
        z_score,
        z_score > LUX_ZSCORE_THRESHOLD,
    )


# ============================================================
# CLASSIFICAZIONE TICK BUY / SELL
# ============================================================

def classify_trade_aggression(price):
    """
    Classificazione tick-rule.

    Prezzo superiore al tick precedente:
        BUY

    Prezzo inferiore al tick precedente:
        SELL

    Prezzo uguale:
        nessuna nuova direzione.

    IMPORTANTE:
    questo NON è un vero Bid/Ask footprint.
    È una classificazione basata sul movimento del prezzo.
    """

    global last_processed_price

    if last_processed_price is None:
        last_processed_price = price
        return None

    if price > last_processed_price:
        direction = True

    elif price < last_processed_price:
        direction = False

    else:
        direction = None

    last_processed_price = price

    return direction


# ============================================================
# CONTROLLO LIQUIDITY SWEEP / IMBALANCE
# ============================================================

def check_luxalgo_manipulation(
    tf,
    candle,
    price,
    is_buy_aggression,
    volume_ticks,
):
    """
    Controlla:

    1. Volume spike
    2. Imbalance
    3. Sweep del massimo/minimo precedente
    4. Rientro nel range
    """

    # --------------------------------------------------------
    # AGGIORNAMENTO VOLUME
    # --------------------------------------------------------

    if is_buy_aggression is True:

        candle.buy_vol += volume_ticks
        candle.buy_ticks += 1

    elif is_buy_aggression is False:

        candle.sell_vol += volume_ticks
        candle.sell_ticks += 1


    # --------------------------------------------------------
    # AGGIORNAMENTO OHLC
    # --------------------------------------------------------

    candle.close = price

    candle.high = max(
        candle.high,
        price,
    )

    candle.low = min(
        candle.low,
        price,
    )


    # --------------------------------------------------------
    # SERVONO ALMENO 10 CANDELE STORICHE
    # --------------------------------------------------------

    if (
        len(high_history[tf])
        < LUX_PIVOT_LOOKBACK
        or
        len(low_history[tf])
        < LUX_PIVOT_LOOKBACK
    ):
        return


    # --------------------------------------------------------
    # LIQUIDITY HIGH / LOW
    # --------------------------------------------------------

    liquidity_high = max(
        high_history[tf]
    )

    liquidity_low = min(
        low_history[tf]
    )


    # --------------------------------------------------------
    # VOLUME
    # --------------------------------------------------------

    total_vol = (
        candle.buy_vol
        + candle.sell_vol
    )

    z_score, is_volume_spike = (
        calculate_volume_metrics(
            tf,
            total_vol,
        )
    )


    if not is_volume_spike:
        return


    if candle.alerted:
        return


    # --------------------------------------------------------
    # DELTA
    # --------------------------------------------------------

    delta_net = (
        candle.buy_vol
        - candle.sell_vol
    )


    # --------------------------------------------------------
    # IMBALANCE
    # --------------------------------------------------------

    if (
        candle.sell_vol > 0
        and candle.buy_vol > 0
    ):

        if delta_net > 0:

            ratio = (
                candle.buy_vol
                / candle.sell_vol
            )

        elif delta_net < 0:

            ratio = (
                candle.sell_vol
                / candle.buy_vol
            )

        else:

            ratio = 1.0

    else:
        # Non abbiamo entrambi i lati.
        return


    if ratio < LUX_DELTA_RATIO:
        return


    # ========================================================
    # SCENARIO A
    # SWEEP MASSIMO + RIENTRO
    # ========================================================

    detected_bubble = None

    if (
        candle.high > liquidity_high
        and candle.close <= liquidity_high
        and delta_net > 0
    ):

        detected_bubble = {

            "icon": "🟢 🫧",

            "type": (
                "CACCIA AI MASSIMI "
                "(Trapped Buyers)"
            ),

            "details": (
                "Sweep del massimo precedente "
                "seguito da rientro nel range. "
                "Il flusso BUY è dominante."
            ),

            "color_bias": (
                "SHORT "
                "(Inversione Ribassista)"
            ),

            "pivot": liquidity_high,
        }


    # ========================================================
    # SCENARIO B
    # SWEEP MINIMO + RIENTRO
    # ========================================================

    elif (
        candle.low < liquidity_low
        and candle.close >= liquidity_low
        and delta_net < 0
    ):

        detected_bubble = {

            "icon": "🔴 🫧",

            "type": (
                "CACCIA AI MINIMI "
                "(Trapped Sellers)"
            ),

            "details": (
                "Sweep del minimo precedente "
                "seguito da rientro nel range. "
                "Il flusso SELL è dominante."
            ),

            "color_bias": (
                "LONG "
                "(Inversione Rialzista)"
            ),

            "pivot": liquidity_low,
        }


    # ========================================================
    # NESSUN SEGNALE
    # ========================================================

    if detected_bubble is None:
        return


    # Evita duplicati nella stessa candela.
    candle.alerted = True


    # ========================================================
    # MESSAGGIO TELEGRAM
    # ========================================================

    msg = (
        f"{detected_bubble['icon']} "
        f"*LUXALGO ORDER FLOW ALERT*\n"

        f"• *Strumento:* XAU/USD\n"

        f"• *Timeframe:* {tf}\n"

        f"• *Segnale:* "
        f"{detected_bubble['type']}\n\n"

        f"⚠️ *Analisi:*\n"

        f"_{detected_bubble['details']}_\n\n"

        f"📊 *Metriche:*\n"

        f"• *Prezzo di rientro:* "
        f"{candle.close}\n"

        f"• *Pivot violato:* "
        f"{detected_bubble['pivot']}\n"

        f"• *Delta netto:* "
        f"{delta_net:+.2f}\n"

        f"• *Imbalance ratio:* "
        f"{ratio:.2f}x\n"

        f"• *Volume Z-Score:* "
        f"{z_score:.2f}\n\n"

        f"⚠️ *Nota:* "
        f"BUY/SELL è classificato con tick-rule."
    )


    send_telegram_alert_instant(msg)


# ============================================================
# AGGIORNAMENTO MARKET FLOW
# ============================================================

def update_market_flow(
    price,
    volume_ticks,
    timestamp_ms,
):
    if price is None:
        return


    try:
        price = float(price)

        volume_ticks = float(
            volume_ticks or 1.0
        )

        timestamp_sec = (
            float(timestamp_ms)
            / 1000.0
        )

    except (
        TypeError,
        ValueError,
    ):
        return


    # --------------------------------------------------------
    # CLASSIFICA TICK
    # --------------------------------------------------------

    direction = classify_trade_aggression(
        price
    )


    # --------------------------------------------------------
    # AGGIORNA OGNI TIMEFRAME
    # --------------------------------------------------------

    for tf, duration in timeframes.items():

        period_index = int(
            timestamp_sec
            // duration
        )


        with state_lock:

            candle = candles_data[tf]


            # ------------------------------------------------
            # NUOVA CANDELA
            # ------------------------------------------------

            if (
                candle.current_period is None
                or
                period_index
                != candle.current_period
            ):

                # Salva la candela appena conclusa.
                if (
                    candle.current_period is not None
                    and candle.open is not None
                    and candle.close is not None
                ):

                    completed_volume = (
                        candle.buy_vol
                        + candle.sell_vol
                    )

                    volume_history[tf].append(
                        completed_volume
                    )

                    high_history[tf].append(
                        candle.high
                    )

                    low_history[tf].append(
                        candle.low
                    )


                # Crea nuova candela.
                candle.reset(
                    period_index,
                    price,
                )


                # Il primo tick viene conteggiato.
                if direction is True:

                    candle.buy_vol += (
                        volume_ticks
                    )

                    candle.buy_ticks += 1


                elif direction is False:

                    candle.sell_vol += (
                        volume_ticks
                    )

                    candle.sell_ticks += 1


                continue


            # ------------------------------------------------
            # CANDELA ESISTENTE
            # ------------------------------------------------

            check_luxalgo_manipulation(
                tf=tf,
                candle=candle,
                price=price,
                is_buy_aggression=direction,
                volume_ticks=volume_ticks,
            )


# ============================================================
# FINNHUB WEBSOCKET - MESSAGGI
# ============================================================

def on_message(ws, message):

    try:
        data = json.loads(message)

    except json.JSONDecodeError:

        print(
            "⚠️ [WebSocket] JSON non valido:",
            message,
            flush=True,
        )

        return


    # --------------------------------------------------------
    # EVENTI NON TRADE
    # --------------------------------------------------------

    if data.get("type") != "trade":
        return


    # --------------------------------------------------------
    # TICK
    # --------------------------------------------------------

    for tick in data.get(
        "data",
        [],
    ):

        try:

            price = tick.get("p")

            volume = tick.get(
                "v",
                1,
            )

            timestamp = tick.get("t")


            if (
                price is None
                or
                timestamp is None
            ):
                continue


            update_market_flow(
                price=price,
                volume_ticks=volume,
                timestamp_ms=timestamp,
            )


        except Exception as exc:

            print(
                f"❌ [Tick Error] {exc}",
                flush=True,
            )


# ============================================================
# FINNHUB WEBSOCKET - ERROR
# ============================================================

def on_error(
    ws,
    error,
):

    print(
        f"⚠️ [WebSocket Error] {error}",
        flush=True,
    )


# ============================================================
# FINNHUB WEBSOCKET - CLOSE
# ============================================================

def on_close(
    ws,
    close_status_code,
    close_msg,
):

    print(
        "🔄 [WebSocket] Connessione chiusa "
        f"(code={close_status_code}, "
        f"msg={close_msg})",
        flush=True,
    )


# ============================================================
# FINNHUB WEBSOCKET - OPEN
# ============================================================

def on_open(ws):

    print(
        f"📡 [WebSocket] Connesso a Finnhub. "
        f"Streaming {SYMBOL} attivo.",
        flush=True,
    )


    # --------------------------------------------------------
    # SUBSCRIBE ORO
    # --------------------------------------------------------

    ws.send(
        json.dumps(
            {
                "type": "subscribe",
                "symbol": SYMBOL,
            }
        )
    )


    # --------------------------------------------------------
    # TELEGRAM ONLINE
    # --------------------------------------------------------

    send_telegram_alert_instant(
        "🫧 *LUXALGO ORDER FLOW ONLINE*\n\n"
        "Motore attivo su *XAU/USD*.\n\n"
        "Monitoraggio liquidità, "
        "sweep e spike volumetrici avviato."
    )


# ============================================================
# AVVIO WEBSOCKET
# ============================================================

def start_websocket():

    if not FINNHUB_TOKEN:

        raise RuntimeError(
            "FINNHUB_TOKEN non configurato "
            "nelle Environment Variables."
        )


    while True:

        try:

            print(
                "🔌 [WebSocket] "
                "Avvio connessione...",
                flush=True,
            )


            ws = websocket.WebSocketApp(

                f"wss://ws.finnhub.io"
                f"?token={FINNHUB_TOKEN}",

                on_open=on_open,

                on_message=on_message,

                on_error=on_error,

                on_close=on_close,
            )


            ws.run_forever(

                ping_interval=PING_INTERVAL,

                ping_timeout=PING_TIMEOUT,
            )


        except Exception as exc:

            print(
                f"❌ [WebSocket] "
                f"Eccezione: {exc}",
                flush=True,
            )


        print(
            f"🔁 [WebSocket] "
            f"Riconnessione tra "
            f"{RECONNECT_SECONDS} secondi...",
            flush=True,
        )


        time.sleep(
            RECONNECT_SECONDS
        )


# ============================================================
# HEALTH CHECK SERVER
# ============================================================

class HealthCheckServer(
    BaseHTTPRequestHandler
):

    def send_ok(self):

        self.send_response(200)

        self.send_header(
            "Content-Type",
            "text/plain; charset=utf-8",
        )

        self.end_headers()

        self.wfile.write(
            b"LuxAlgo Volumetric Engine Awake\n"
        )


    def do_HEAD(self):

        self.send_ok()


    def do_GET(self):

        self.send_ok()


    def log_message(
        self,
        format,
        *args,
    ):
        # Evita spam nei log Render.
        return


# ============================================================
# AVVIO HEALTH SERVER
# ============================================================

def run_health_server():

    server = ThreadingHTTPServer(
        (
            "0.0.0.0",
            PORT,
        ),
        HealthCheckServer,
    )


    print(
        f"🌐 [Health] Server HTTP "
        f"in ascolto sulla porta {PORT}.",
        flush=True,
    )


    try:

        server.serve_forever()

    except Exception as exc:

        print(
            f"❌ [Health] Server terminato: "
            f"{exc}",
            flush=True,
        )

    finally:

        server.server_close()


# ============================================================
# SELF PING RENDER
# ============================================================

def self_ping():

    time.sleep(30)


    if not RENDER_APP_NAME:

        print(
            "ℹ️ [Anti-Sleep] "
            "RENDER_EXTERNAL_HOSTNAME "
            "non configurato.",
            flush=True,
        )

        return


    host = RENDER_APP_NAME.strip()


    if not host.startswith(
        (
            "http://",
            "https://",
        )
    ):

        host = (
            f"https://{host}"
        )


    health_url = (
        f"{host.rstrip('/')}/"
    )


    while True:

        try:

            response = requests.get(
                health_url,
                timeout=10,
            )


            print(
                f"💤 [Anti-Sleep] "
                f"HTTP {response.status_code}",
                flush=True,
            )


        except requests.RequestException as exc:

            print(
                f"⚠️ [Anti-Sleep] "
                f"Ping fallito: {exc}",
                flush=True,
            )


        time.sleep(600)


# ============================================================
# MAIN
# ============================================================

def main():

    print(
        "🚀 [System] "
        "Inizializzazione LuxAlgo "
        "Volumetric Engine...",
        flush=True,
    )


    # --------------------------------------------------------
    # TELEGRAM WORKER
    # --------------------------------------------------------

    threading.Thread(
        target=telegram_worker,
        name="TelegramWorker",
        daemon=True,
    ).start()


    # --------------------------------------------------------
    # HEALTH SERVER
    # --------------------------------------------------------

    threading.Thread(
        target=run_health_server,
        name="HealthServer",
        daemon=True,
    ).start()


    # --------------------------------------------------------
    # SELF PING
    # --------------------------------------------------------

    threading.Thread(
        target=self_ping,
        name="SelfPing",
        daemon=True,
    ).start()


    # --------------------------------------------------------
    # WEBSOCKET
    # --------------------------------------------------------

    start_websocket()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()
