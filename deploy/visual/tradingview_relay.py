"""Relay existing TradingView bubble alerts to Telegram with a durable outbox."""
import hashlib
import hmac
import json
import logging
import math
import os
import re
import threading
import time
import urllib.request
import uuid
from urllib.parse import urlsplit

TEST_MESSAGE = '🧪 TEST COLLEGAMENTO TradingView → Render → Telegram. Nessun segnale operativo.'
PATTERN = re.compile(r'^(LONG 🟢|SHORT 🔴) \| OANDA:XAUUSD \| TF (3|5|15|30|45|60|240|D|1D) \| Bubble appena comparsa \| Prezzo ([0-9]+(?:\.[0-9]+)?)$')


def parse_message(raw):
    message = raw.decode('utf-8').strip()
    if message == TEST_MESSAGE:
        return message, 'TEST'
    match = PATTERN.fullmatch(message)
    if not match or not math.isfinite(float(match[3])) or float(match[3]) <= 0:
        raise ValueError('invalid_bubble')
    return message, match[2]


class Relay:
    def __init__(self, database_url, token, chat_id, secret):
        import psycopg
        self.connect = lambda: psycopg.connect(database_url, connect_timeout=2)
        self.token, self.chat_id, self.secret = token, chat_id, secret
        self.last_error = None
        self.stop = threading.Event()
        with self.connect() as conn:
            conn.execute('''CREATE TABLE IF NOT EXISTS tv_bubble_notifications (
                id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, message TEXT NOT NULL,
                timeframe TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'queued',
                received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                message_id BIGINT, completed_at TIMESTAMPTZ)''')
            conn.execute("UPDATE tv_bubble_notifications SET state='unknown', completed_at=now() WHERE state='sending'")
        threading.Thread(target=self.worker, daemon=True).start()

    def accept(self, raw):
        message, timeframe = parse_message(raw)
        fingerprint = hashlib.sha256(message.encode()).hexdigest()
        identifier = str(uuid.uuid4())
        with self.connect() as conn:
            # Serialize identical messages across simultaneous TradingView deliveries.
            conn.execute('SELECT pg_advisory_xact_lock(%s)', (int(fingerprint[:15], 16),))
            duplicate = conn.execute("SELECT id FROM tv_bubble_notifications WHERE fingerprint=%s AND received_at > now() - interval '10 seconds' LIMIT 1", (fingerprint,)).fetchone()
            if duplicate:
                return {'accepted': True, 'duplicate': True, 'id': duplicate[0]}
            conn.execute('INSERT INTO tv_bubble_notifications (id,fingerprint,message,timeframe) VALUES (%s,%s,%s,%s)', (identifier, fingerprint, message, timeframe))
        return {'accepted': True, 'duplicate': False, 'id': identifier}

    def worker(self):
        conn = None
        while not self.stop.is_set():
            try:
                if conn is None or conn.closed:
                    conn = self.connect()
                    conn.autocommit = True
                with conn.transaction():
                    row = conn.execute("SELECT id,message FROM tv_bubble_notifications WHERE state='queued' ORDER BY received_at FOR UPDATE SKIP LOCKED LIMIT 1").fetchone()
                    if row:
                        conn.execute("UPDATE tv_bubble_notifications SET state='sending' WHERE id=%s", (row[0],))
                if not row:
                    self.stop.wait(1)
                    continue
                state, message_id = 'unknown', None
                try:
                    request = urllib.request.Request('https://api.telegram.org/bot' + self.token + '/sendMessage', data=json.dumps({'chat_id': self.chat_id, 'text': row[1]}).encode(), headers={'Content-Type': 'application/json'})
                    with urllib.request.urlopen(request, timeout=10) as response:
                        result = json.load(response)
                    if result.get('ok') and isinstance(result.get('result', {}).get('message_id'), int):
                        state, message_id = 'api_confirmed', result['result']['message_id']
                    else:
                        state = 'api_rejected'
                except Exception:
                    pass  # Telegram URLs contain the token: never log exceptions.
                conn.execute('UPDATE tv_bubble_notifications SET state=%s,message_id=%s,completed_at=now() WHERE id=%s', (state, message_id, row[0]))
                logging.info('tv_relay_result event=%s state=%s message_id=%s', row[0], state, message_id)
                self.last_error = None
            except Exception as exc:
                if conn is not None:
                    conn.close()
                    conn = None
                self.last_error = type(exc).__name__
                logging.error('tv_relay_worker_error type=%s', self.last_error)
                self.stop.wait(5)

    def status(self):
        with self.connect() as conn:
            counts = dict(conn.execute('SELECT state,count(*) FROM tv_bubble_notifications GROUP BY state').fetchall())
        return {'configured': True, 'storage': 'postgres', 'counts': counts, 'worker_error': self.last_error}


def receive_http(handler, relay):
    path = urlsplit(handler.path).path
    prefix = '/webhooks/tradingview/'
    if not path.startswith(prefix):
        return False
    provided = path[len(prefix):]
    if relay is None:
        handler.reply(503, {'error': 'relay_not_configured'})
        return True
    if len(relay.secret) < 32 or not hmac.compare_digest(provided.encode(), relay.secret.encode()):
        handler.reply(401, {'error': 'unauthorized'})
        return True
    try:
        length = int(handler.headers.get('Content-Length', '0'))
        if not 0 < length <= 1024:
            handler.reply(413, {'error': 'invalid_body_size'})
        else:
            handler.reply(202, relay.accept(handler.rfile.read(length)))
    except (ValueError, UnicodeError):
        handler.reply(400, {'error': 'invalid_bubble'})
    except Exception:
        handler.reply(503, {'error': 'storage_unavailable'})
    return True


def configured_relay():
    values = [os.getenv(key, '') for key in ('TV_RELAY_DATABASE_URL','TELEGRAM_BOT_TOKEN','TELEGRAM_CHAT_ID','TV_RELAY_SECRET')]
    if all(values) and len(values[-1]) >= 32:
        return Relay(*values)
    return None
