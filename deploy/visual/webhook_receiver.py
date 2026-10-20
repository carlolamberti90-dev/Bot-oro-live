"""Authenticated, notification-only TradingView ingress; no trading API."""
import hashlib
import hmac
import json
import math
import os
import queue
import threading
import time
import urllib.request

ALLOWED = {'3', '5', '15', '30', '45', '60', '240', 'D', '1D'}


def validate(payload, secret, now=None):
    if not isinstance(payload, dict):
        raise ValueError('invalid_payload')
    if len(secret) < 32 or not hmac.compare_digest(str(payload.get('secret', '')).encode(), secret.encode()):
        raise PermissionError('unauthorized')
    if payload.get('symbol') != 'OANDA:XAUUSD' or str(payload.get('timeframe')) not in ALLOWED:
        raise ValueError('symbol_or_timeframe_rejected')
    if payload.get('direction') not in ('LONG', 'SHORT'):
        raise ValueError('direction_rejected')
    price = float(payload['price'])
    candle = int(payload['candle_time'])
    observed = int(payload['observed_at'])
    now = time.time() if now is None else now
    if not math.isfinite(price) or price <= 0 or abs(now - observed / 1000) > 300 or candle > observed:
        raise ValueError('invalid_price_or_timestamp')
    event_id = str(payload['event_id'])
    if not 1 <= len(event_id) <= 200:
        raise ValueError('invalid_event_id')
    # Explicit allowlist: never retain the shared secret in event data.
    return dict(symbol=payload['symbol'], timeframe=str(payload['timeframe']),
                direction=payload['direction'], price=price, candle_time=candle,
                observed_at=observed, event_id=event_id)


class Receiver:
    """Bounded asynchronous ingress, persistent deduplication/outbox in SQLite.

    The configured path MUST be on durable storage before production enablement.
    A Telegram timeout is recorded as unknown, not automatically retried: Telegram
    sendMessage has no idempotency key, so a retry can duplicate a delivered message.
    """
    def __init__(self, database, token, chat_id, secret):
        import sqlite3
        self.db = sqlite3.connect(database, check_same_thread=False)
        self.db.execute('CREATE TABLE IF NOT EXISTS tv_webhook_events (id TEXT PRIMARY KEY, payload TEXT NOT NULL, state TEXT NOT NULL, received REAL NOT NULL, message_id INTEGER)')
        self.db.commit()
        self.lock = threading.Lock()
        self.token, self.chat_id, self.secret = token, chat_id, secret
        self.jobs = queue.Queue(maxsize=100)
        with self.lock:
            pending = self.db.execute("SELECT id FROM tv_webhook_events WHERE state='queued' LIMIT 100").fetchall()
            # An interrupted in-flight request cannot safely be automatically retried.
            self.db.execute("UPDATE tv_webhook_events SET state='unknown' WHERE state='sending'")
            self.db.commit()
        for row in pending:
            self.jobs.put_nowait(row[0])
        threading.Thread(target=self.worker, daemon=True).start()

    def accept(self, payload):
        event = validate(payload, self.secret)
        key = hashlib.sha256(event['event_id'].encode()).hexdigest()
        with self.lock:
            previous = self.db.execute('SELECT state FROM tv_webhook_events WHERE id=?', (key,)).fetchone()
            if previous:
                return {'accepted': True, 'duplicate': True, 'state': previous[0]}
            if self.jobs.full():
                raise RuntimeError('queue_full')
            self.db.execute('INSERT INTO tv_webhook_events VALUES (?,?,?,?,NULL)',
                            (key, json.dumps(event), 'queued', time.time()))
            self.db.commit()
            self.jobs.put_nowait(key)
        return {'accepted': True, 'duplicate': False, 'state': 'queued'}

    def worker(self):
        import logging
        while True:
            key = self.jobs.get()
            with self.lock:
                raw = self.db.execute('SELECT payload FROM tv_webhook_events WHERE id=?', (key,)).fetchone()[0]
                self.db.execute("UPDATE tv_webhook_events SET state='sending' WHERE id=?", (key,))
                self.db.commit()
            event = json.loads(raw)
            state, message_id = 'unknown', None
            try:
                text = ('LONG 🟢' if event['direction'] == 'LONG' else 'SHORT 🔴') + ' | OANDA:XAUUSD | TF ' + event['timeframe'] + ' | Bubble intrabar | Prezzo ' + str(event['price'])
                request = urllib.request.Request('https://api.telegram.org/bot' + self.token + '/sendMessage',
                    data=json.dumps({'chat_id': self.chat_id, 'text': text}).encode(),
                    headers={'Content-Type': 'application/json'})
                with urllib.request.urlopen(request, timeout=10) as response:
                    result = json.load(response)
                if result.get('ok') is True and isinstance(result.get('result', {}).get('message_id'), int):
                    state, message_id = 'api_confirmed', result['result']['message_id']
                else:
                    state = 'api_rejected'
            except Exception:
                pass  # Never log exception URLs, which contain Telegram credentials.
            with self.lock:
                self.db.execute('UPDATE tv_webhook_events SET state=?,message_id=? WHERE id=?', (state, message_id, key))
                self.db.commit()
            logging.info('tv_webhook_result event=%s state=%s message_id=%s', key, state, message_id)
            self.jobs.task_done()


def receive_http(handler, receiver):
    """Call before the setup UI's session/Origin check for this exact path only."""
    if handler.path != '/webhooks/tradingview':
        return False
    if receiver is None:
        handler.reply(503, {'error': 'webhook_not_configured'})
        return True
    try:
        length = int(handler.headers.get('Content-Length', '0'))
        if not 0 < length <= 8192:
            handler.reply(413, {'error': 'invalid_body_size'})
            return True
        result = receiver.accept(json.loads(handler.rfile.read(length)))
        handler.reply(202, result)
    except PermissionError:
        handler.reply(401, {'error': 'unauthorized'})
    except (ValueError, TypeError, KeyError, OverflowError):
        handler.reply(400, {'error': 'invalid_event'})
    except Exception:
        handler.reply(503, {'error': 'temporarily_unavailable'})
    return True
