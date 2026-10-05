import os
import json
import time
import threading
import websocket
import requests
import collections
import queue
import re
from http.server import BaseHTTPRequestHandler, HTTPServer

# 1. Configurazione Variabili d'Ambiente protette da Render
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
CHAT_ID_RAW = os.environ.get('TELEGRAM_CHAT_ID')
FINNHUB_TOKEN = os.environ.get('FINNHUB_TOKEN')
RENDER_APP_NAME = os.environ.get('RENDER_EXTERNAL_HOSTNAME')

# Funzione per la pulizia dinamica e formattazione del Chat ID per evitare blocchi nell'invio
def clean_chat_id(raw_id):
    if not raw_id:
        return None
    cleaned = str(raw_id).strip()
    if re.search(r'[a-zA-Z]', cleaned) and not cleaned.startswith('@'):
        return f"@{cleaned}"
    return cleaned

CHAT_ID = clean_chat_id(CHAT_ID_RAW)

# Parametri Algoritmici Istituzionali di LuxAlgo Order Flow
LUX_PIVOT_LOOKBACK = 10     # Candele passate per identificare i Pivot di liquidità (Caccie agli Stop)
LUX_ZSCORE_THRESHOLD = 2.0  # Deviazione statistica minima per validare lo spike volumetrico della bolla
LUX_DELTA_RATIO = 3.0       # Rapporto di sbilanciamento minimo d'asta Bid/Ask (3:1) richiesto

# Coda asincrona per l'invio degli alert senza rallentare il WebSocket dei tick
telegram_queue = queue.Queue()

# Struttura dati istituzionale OHLCV + Order Flow (Buy vs Sell Volume) per ogni Timeframe richiesto
class VolumetricCandle:
    def __init__(self):
        self.open = None
        self.high = -1
        self.low = 1e9
        self.close = None
        self.buy_vol = 0
        self.sell_vol = 0
        self.current_period = None
        self.alerted = False

# Mappatura completa di tutti i timeframe della tua discussione originaria
timeframes = {'M1': 60, 'M3': 180, 'M5': 300, '15m': 900, '30m': 1800, '1h': 3600, '4h': 14400, 'D1': 86400}
candles_data = {tf: VolumetricCandle() for tf in timeframes.keys()}

# Database storici per i calcoli statistici di LuxAlgo
volume_history = {tf: collections.deque(maxlen=150) for tf in timeframes.keys()}
high_history = {tf: collections.deque(maxlen=LUX_PIVOT_LOOKBACK) for tf in timeframes.keys()}
low_history = {tf: collections.deque(maxlen=LUX_PIVOT_LOOKBACK) for tf in timeframes.keys()}

last_processed_price = None
alert_lock = threading.Lock()

def telegram_worker():
    """Thread Worker dedicato: invia i messaggi in background eliminando ogni latenza di rete"""
    url = f"https://telegram.org{TELEGRAM_TOKEN}/sendMessage"
    while True:
        try:
            message = telegram_queue.get()
            payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"}
            
            response = requests.post(url, json=payload, timeout=8)
            if response.status_code != 200:
                print(f"❌ [Telegram Error] Codice {response.status_code}: {response.text}", flush=True)
                if "bad request" in response.text.lower():
                    payload.pop("parse_mode", None)
                    requests.post(url, json=payload, timeout=5)
            else:
                print("⚡ [Telegram] Notifica consegnata con successo a destinazione.", flush=True)
        except Exception as e:
            print(f"❌ [Telegram Exception] Errore di connessione: {e}", flush=True)
        finally:
            telegram_queue.task_done()

def send_telegram_alert_instant(message):
    """Accoda istantaneamente l'alert per l'invio flash"""
    telegram_queue.put(message)

def calculate_volume_metrics(tf, total_vol):
    history = volume_history[tf]
    if len(history) < 20:
        return 0.0, False
    mean = sum(history) / len(history)
    variance = sum((x - mean) ** 2 for x in history) / len(history)
    std_dev = variance ** 0.5
    z_score = (total_vol - mean) / std_dev if std_dev != 0 else 0.0
    return z_score, (z_score > LUX_ZSCORE_THRESHOLD)

def check_luxalgo_manipulation(tf, c, price, is_buy_aggression, volume_ticks):
    """Rilevatore in tempo reale delle bolle di manipolazione e assorbimento LuxAlgo"""
    if is_buy_aggression:
        c.buy_vol += volume_ticks
    else:
        c.sell_vol += volume_ticks
        
    c.close = price
    if price > c.high: c.high = price
    if price < c.low: c.low = price

    if len(high_history[tf]) < LUX_PIVOT_LOOKBACK or len(low_history[tf]) < LUX_PIVOT_LOOKBACK:
        return

    lux_liquidity_high = max(high_history[tf])
    lux_liquidity_low = min(low_history[tf])

    total_vol = c.buy_vol + c.sell_vol
    z_score, is_volume_spike = calculate_volume_metrics(tf, total_vol)

    if not is_volume_spike or c.alerted:
        return

    delta_netto = c.buy_vol - c.sell_vol
    
    if c.sell_vol > 0 and c.buy_vol > 0:
        ratio = (c.buy_vol / c.sell_vol) if delta_netto > 0 else (c.sell_vol / c.buy_vol)
    else:
        ratio = float(total_vol)

    if ratio < LUX_DELTA_RATIO:
        return

    detected_bubble = None

    # Scenario A: Bolla Verde LuxAlgo (Raid sui Massimi con Trapped Buyers / Assorbimento)
    if c.high > lux_liquidity_high and c.close <= lux_liquidity_high:
        detected_bubble = {
            "icon": "🟢 🫧",
            "type": "CACCIA AI MASSIMI (Trapped Buyers)",
            "details": "Rottura del Pivot High fallita. Gli istituzionali hanno assorbito i Buy Stop retail innescando trappole.",
            "color_bias": "SHORT (Inversione Ribassista)"
        }

    # Scenario B: Bolla Rossa LuxAlgo (Raid sui Minimi con Trapped Sellers / Accumulazione)
    elif c.low < lux_liquidity_low and c.close >= lux_liquidity_low:
        detected_bubble = {
            "icon": "🔴 🫧",
            "type": "CACCIA AI MINIMI (Trapped Sellers)",
            "details": "Rottura del Pivot Low fallita. Gli istituzionali hanno accumulato bloccando il breakout short.",
            "color_bias": "LONG (Inversione Rialzista)"
        }

    if detected_bubble:
        c.alerted = True
        
        msg = f"{detected_bubble['icon']} *LUXALGO ORDER FLOW CRITICAL ALERT*\n"
        msg += f"• *Strumento:* XAU/USD (Oro)\n"
        msg += f"• *Timeframe:* {tf}\n"
        msg += f"• *Segnale:* {detected_bubble['type']}\n\n"
        msg += f"⚠️ *Analisi Strutturale:*\n"
        msg += f"_{detected_bubble['details']}_\n\n"
        msg += f"📊 *Metriche del Flusso dell'Asta:*\n"
        msg += f"• *Prezzo di Rientro:* {c.close}\n"
        msg += f"• *Pivot Range Violato:* {lux_liquidity_high if 'Buyers' in detected_bubble['type'] else lux_liquidity_low}\n"
        msg += f"• *Delta Netto:* {delta_netto:+d} ({detected_bubble['color_bias']})\n"
        msg += f"• *Squilibrio (Imbalance Ratio):* {ratio:.2f}x\n"
        msg += f"• *Volume Z-Score (Dimensione Bolla):* {z_score:.2f}"
        
        send_telegram_alert_instant(msg)

def update_market_flow(price, volume_ticks, timestamp_ms):
    global last_processed_price
    
    if last_processed_price is None:
        last_processed_price = price
        return

    is_buy_aggression = price >= last_processed_price
    last_processed_price = price
    timestamp_sec = timestamp_ms / 1000.0

    for tf, duration in timeframes.items():
        c = candles_data[tf]
        period_index = int(timestamp_sec // duration)
        
        if c.current_period is None or period_index > c.current_period:
            if c.current_period is not None and c.open is not None:
                with alert_lock:
                    volume_history[tf].append(c.buy_vol + c.sell_vol)
                    high_history[tf].append(c.high)
                    low_history[tf].append(c.low)
                
            c.current_period = period_index
            c.open = price; c.high = price; c.low = price; c.close = price
            c.buy_vol = volume_ticks if is_buy_aggression else 0
            c.sell_vol = 0 if is_buy_aggression else volume_ticks
            c.alerted = False
        else:
            check_luxalgo_manipulation(tf, c, price, is_buy_aggression, volume_ticks)

def on_message(ws, message):
    data = json.loads(message)
    if data.get('type') == 'trade':
        for tick in data['data']:
            update_market_flow(tick['p'], tick.get('v', 1), tick['t'])

def on_error(ws, error):
    print(f"⚠️ [WebSocket Error] Connessione interrotta: {error}", flush=True)

def on_close(ws, close_status_code, close_msg):
    print("🔄 [WebSocket] Chiusura feed. Riconnessione automatica tra 5 secondi...", flush=True)
    time.sleep(5)
    start_websocket()

def on_open(ws):
    ws.send(json.dumps({"type": "subscribe", "symbol": "OANDA:XAU_USD"}))
    print("📡 [WebSocket] Connessione stabilita con Finnhub. Streaming XAU/USD attivo.", flush=True)
    send_telegram_alert_instant("🫧 *LUXALGO ORDER FLOW CLONE ONLINE*\nIl motore quantistico ad alta velocità è attivo su *XAU/USD (Oro)*.\n\nPronto a trasmettere caccie alla liquidità ed assorbimenti istituzionali in tempo reale.")

def start_websocket():
    ws = websocket.WebSocketApp(
        f"wss://ws.finnhub.io?token={FINNHUB_TOKEN}",
        on_message=on_message, on_error=on_error, on_close=on_close
    )
    ws.on_open = on_open
    ws.run_forever()

def self_ping():
    time.sleep(30)
    while True:
        if RENDER_APP_NAME:
            try: 
                requests.get(f"https://{RENDER_APP_NAME}", timeout=10)
                print("💤 [Anti-Sleep] Auto-ping inviato correttamente.", flush=True)
            except Exception: 
                pass
        time.sleep(600)

class HealthCheckServer(BaseHTTPRequestHandler):
    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"LuxAlgo Volumetric Engine Awake")

def run_health_server():
    server = HTTPServer(("0.0.0.0", 10000), HealthCheckServer)
    server.serve_forever()

if __name__ == "__main__":
Usa il codice con cautela.
print("🚀 [System] Inizializzazione moduli asincroni dell'applicazione...", flush=True)
# 1. Avvia il thread asincrono per l'invio immediato a Telegram
threading.Thread(target=telegram_worker, daemon=True).start()
# 2. Avvia il server HTTP in un thread DAEMON separato per non bloccare lo script
threading.Thread(target=run_health_server, daemon=True).start()
# 3. Avvia il sistema anti-standby
threading.Thread(target=self_ping, daemon=True).start()
# 4. Esegue il WebSocket sul thread principale
start_websocket()
