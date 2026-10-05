import os
import json
import time
import threading
import websocket
import requests
import collections
from http.server import BaseHTTPRequestHandler, HTTPServer

# 1. Configurazione Variabili d'Ambiente protette
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
FINNHUB_TOKEN = os.environ.get('FINNHUB_TOKEN')
RENDER_APP_NAME = os.environ.get('RENDER_EXTERNAL_HOSTNAME')

# Struttura dati istituzionale OHLCV per tracciare le candele reali
candles_data = {
    'M1':  {'open': None, 'high': -1, 'low': 1e9, 'close': None, 'volume': 0, 'current_period': None, 'alerted': False},
    'M3':  {'open': None, 'high': -1, 'low': 1e9, 'close': None, 'volume': 0, 'current_period': None, 'alerted': False},
    'M5':  {'open': None, 'high': -1, 'low': 1e9, 'close': None, 'volume': 0, 'current_period': None, 'alerted': False},
    '15m': {'open': None, 'high': -1, 'low': 1e9, 'close': None, 'volume': 0, 'current_period': None, 'alerted': False},
    '30m': {'open': None, 'high': -1, 'low': 1e9, 'close': None, 'volume': 0, 'current_period': None, 'alerted': False},
    '1h':  {'open': None, 'high': -1, 'low': 1e9, 'close': None, 'volume': 0, 'current_period': None, 'alerted': False},
    '4h':  {'open': None, 'high': -1, 'low': 1e9, 'close': None, 'volume': 0, 'current_period': None, 'alerted': False},
    'D1':  {'open': None, 'high': -1, 'low': 1e9, 'close': None, 'volume': 0, 'current_period': None, 'alerted': False}
}

# Memoria dinamica per calcolare la velocità dei tick (ultimi 10 minuti)
tick_history = collections.deque(maxlen=600)
current_second_ticks = 0
last_tick_time = int(time.time())
triggered_alerts = {}
alert_lock = threading.Lock()

def send_telegram_alert(message):
    """Invia le notifiche in tempo reale su Telegram"""
    url = f"https://telegram.org{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try: requests.post(url, json=payload)
    except Exception as e: print(f"Errore Telegram: {e}")

def process_instant_alerts():
    """Analizza le anomalie incrociate e pulisce i falsi positivi"""
    global triggered_alerts
    while True:
        time.sleep(0.3)  # Massima reattività sui millisecondi
        with alert_lock:
            if triggered_alerts:
                veloci_colpiti = {'M1', 'M3', 'M5'}.issubset(triggered_alerts.keys())
                msg = ""
                if veloci_colpiti:
                    msg += "🔥 *🚨 CONFERMA MANIPOLAZIONE ISTANTANEA (M1+M3+M5)!!!*\n"
                    msg += "Rilevata anomalia statistica volumetrica simultanea sui timeframe rapidi.\n"
                    msg += f"Strumento: *XAU/USD (Oro)*\n\n"
                    for tf in ['M1', 'M3', 'M5']:
                        data = triggered_alerts.pop(tf)
                        direzione = "🟢 Spinta Up" if data['close'] >= data['open'] else "🔴 Spinta Down"
                        msg += f"⚡️ *{tf}* [{direzione}]\n  • Prezzo: {data['close']} | Vol: {data['volume']} | Z-Score: {data['z_score']:.2f}\n"
                    msg += "\n"
                else:
                    for tf in ['M1', 'M3', 'M5']:
                        if tf in triggered_alerts: triggered_alerts.pop(tf)

                if triggered_alerts:
                    if not msg:
                        msg += "⚠️ *ALERT BOLLA STATISTICA RILEVATO (LIVE)*\n"
                        msg += f"Strumento: *XAU/USD (Oro)*\n\n"
                    for tf, data in triggered_alerts.items():
                        direzione = "🟢 Spinta Up" if data['close'] >= data['open'] else "🔴 Spinta Down"
                        msg += f"📊 *Timeframe {tf}* [{direzione}]\n"
                        msg += f"  • O: {data['open']} | H: {data['high']} | L: {data['low']} | C: {data['close']}\n"
                        msg += f"  • Tick Volume: {data['volume']} | Z-Score: {data['z_score']:.2f}\n\n"
                if msg: send_telegram_alert(msg)
                triggered_alerts.clear()

def update_tick_speed():
    global current_second_ticks, last_tick_time
    now = int(time.time())
    if now > last_tick_time:
        tick_history.append(current_second_ticks)
        current_second_ticks = 0
        last_tick_time = now

def calculate_z_score():
    if len(tick_history) < 30: return 0.0
    n = len(tick_history)
    mean = sum(tick_history) / n
    variance = sum((x - mean) ** 2 for x in tick_history) / n
    std_dev = variance ** 0.5
    return (current_second_ticks - mean) / std_dev if std_dev != 0 else 0.0

def update_candles(price, timestamp_ms):
    global triggered_alerts, current_second_ticks
    current_second_ticks += 1
    update_tick_speed()
    z_score = calculate_z_score()
    timestamp_sec = timestamp_ms / 1000.0
    timeframes = {'M1': 60, 'M3': 180, 'M5': 300, '15m': 900, '30m': 1800, '1h': 3600, '4h': 14400, 'D1': 86400}
    for tf, duration in timeframes.items():
        c = candles_data[tf]
        period_index = int(timestamp_sec // duration)
        if c['current_period'] is None or period_index > c['current_period']:
            c['current_period'] = period_index
            c['open'] = price; c['high'] = price; c['low'] = price; c['close'] = price; c['volume'] = 1; c['alerted'] = False
        else:
            if price > c['high']: c['high'] = price
            if price < c['low']: c['low'] = price
            c['close'] = price
            c['volume'] += 1
            if z_score > 3.0 and not c['alerted']:
                c['alerted'] = True
                with alert_lock:
                    triggered_alerts[tf] = {'open': c['open'], 'high': c['high'], 'low': c['low'], 'close': c['close'], 'volume': c['volume'], 'z_score': z_score}

def on_message(ws, message):
    data = json.loads(message)
    if data.get('type') == 'trade':
        for tick in data['data']: update_candles(tick['p'], tick['t'])

def on_error(ws, error): print(f"Errore WebSocket: {error}")
def on_close(ws, close_status_code, close_msg): time.sleep(5); start_websocket()
def on_open(ws):
    # CORRETTO: Streaming realtime con volumi attivi abilitati su Finnhub
    ws.send(json.dumps({"type": "subscribe", "symbol": "OANDA:XAU_USD"}))
    print("Connessione stabilita con il feed volumetrico reale dell'oro. Bot attivo.")

def start_websocket():
    ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={FINNHUB_TOKEN}", on_message=on_message, on_error=on_error, on_close=on_close)
    ws.on_open = on_open
    ws.run_forever()

def self_ping():
    time.sleep(30)
    while True:
        if RENDER_APP_NAME:
            try: requests.get(f"https://{RENDER_APP_NAME}")
            except Exception: pass
        time.sleep(600)

class HealthCheckServer(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Bot Sveglio e Attivo")

def run_health_server():
    server = HTTPServer(("0.0.0.0", 10000), HealthCheckServer)
    server.serve_forever()

if __name__ == "__main__":
    threading.Thread(target=run_health_server, daemon=True).start()
    threading.Thread(target=self_ping, daemon=True).start()
    threading.Thread(target=process_instant_alerts, daemon=True).start()
    start_websocket()
