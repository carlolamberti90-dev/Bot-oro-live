import os
import json
import time
import hmac
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("xau-pine-webhook")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()
PORT = int(os.getenv("PORT", "10000"))
DEDUP_SECONDS = int(os.getenv("DEDUP_SECONDS", "10"))

last_event = {}
event_lock = threading.Lock()
last_signal_time = None

def validate_config():
    missing = []
    if not TELEGRAM_TOKEN:
        missing.append("TELEGRAM_BOT_TOKEN")
    if not TELEGRAM_CHAT_ID:
        missing.append("TELEGRAM_CHAT_ID")
    if not WEBHOOK_SECRET:
        missing.append("WEBHOOK_SECRET")
    if missing:
        raise RuntimeError("Variabili ambiente mancanti: " + ", ".join(missing))

def telegram_url():
    return f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

def send_telegram(message):
    r = requests.post(
        telegram_url(),
        json={"chat_id": TELEGRAM_CHAT_ID, "text": message},
        timeout=(5, 10),
    )
    r.raise_for_status()

def normalize_payload(payload):
    direction = str(payload.get("direction", "")).upper().strip()
    if direction not in {"LONG", "SHORT"}:
        raise ValueError("direction deve essere LONG o SHORT")

    event_type = str(payload.get("type", "manipulation_bubble")).strip()
    if event_type != "manipulation_bubble":
        raise ValueError("type non supportato")

    ticker = str(payload.get("ticker", "OANDA:XAUUSD")).strip()
    interval = str(payload.get("interval", "")).strip() or "?"
    close = payload.get("close")
    bar_time = str(payload.get("time", "")).strip()

    return {
        "type": event_type,
        "direction": direction,
        "ticker": ticker,
        "interval": interval,
        "close": close,
        "time": bar_time,
    }

def is_duplicate(event):
    key = f'{event["direction"]}|{event["ticker"]}|{event["interval"]}|{event["time"]}|{event["close"]}'
    now = time.time()
    with event_lock:
        cutoff = now - max(DEDUP_SECONDS, 1)
        stale = [k for k, ts in last_event.items() if ts < cutoff]
        for k in stale:
            last_event.pop(k, None)
        if key in last_event:
            return True
        last_event[key] = now
    return False

def format_message(event):
    icon = "🟢🫧" if event["direction"] == "LONG" else "🔴🫧"
    price = event["close"]
    parts = [
        f'{icon} MANIPULATION BUBBLE — {event["direction"]}',
        "",
        event["ticker"],
        f'Timeframe: {event["interval"]}',
    ]
    if price not in (None, ""):
        parts.append(f"Prezzo: {price}")
    if event["time"]:
        parts.append(f'Time: {event["time"]}')
    return "\n".join(parts)

def health_snapshot():
    return {
        "status": "online",
        "engine": "TradingView Pine manipulation-bubble webhook receiver",
        "last_signal_age_seconds": None if last_signal_time is None else round(time.time() - last_signal_time, 1),
        "dedup_seconds": DEDUP_SECONDS,
    }

class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        return

    def _json(self, status, payload):
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()

    def do_GET(self):
        if self.path.split("?", 1)[0] in {"/", "/health"}:
            self._json(200, health_snapshot())
        else:
            self._json(404, {"status": "not_found"})

    def do_POST(self):
        global last_signal_time
        if self.path.split("?", 1)[0] != "/webhook":
            self._json(404, {"status": "not_found"})
            return

        supplied = self.headers.get("X-Webhook-Secret", "")
        query_secret = ""
        if "?" in self.path:
            query = self.path.split("?", 1)[1]
            for part in query.split("&"):
                if part.startswith("secret="):
                    query_secret = part.split("=", 1)[1]
                    break
        supplied = supplied or query_secret
        if not supplied or not hmac.compare_digest(supplied, WEBHOOK_SECRET):
            self._json(401, {"status": "unauthorized"})
            return

        try:
            length = min(int(self.headers.get("Content-Length", "0")), 65536)
            raw = self.rfile.read(length)
            payload = json.loads(raw.decode("utf-8"))
            event = normalize_payload(payload)
        except (ValueError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            self._json(400, {"status": "bad_request", "error": str(exc)})
            return

        if is_duplicate(event):
            self._json(200, {"status": "duplicate_ignored"})
            return

        try:
            send_telegram(format_message(event))
            last_signal_time = time.time()
            log.info("Bubble %s %s inviata", event["interval"], event["direction"])
            self._json(200, {"status": "sent"})
        except requests.RequestException as exc:
            log.error("Telegram error: %s", exc)
            self._json(502, {"status": "telegram_error"})
            
def main():
    validate_config()
    log.info("Avvio ricevitore webhook Pine -> Telegram")
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    server.serve_forever()

if __name__ == "__main__":
    main()
