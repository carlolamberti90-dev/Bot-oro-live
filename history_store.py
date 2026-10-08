"""Durable, compressed checkpoints; local files remain a fallback cache."""
import gzip
import json
import logging
import os
import threading
import time

log = logging.getLogger(__name__)

def merge_checkpoints(old, new):
    """A replacement instance must never erase candles collected by its predecessor."""
    if not old:
        return new
    result = dict(new)
    result['history'] = {}
    replay = []
    for tf, incoming in new.get('history', {}).items():
        incoming_periods = {c['period'] for c in incoming}
        candles = {c['period']: c for c in old.get('history', {}).get(tf, [])}
        candles.update({c['period']: c for c in incoming})
        limit = new.get('history_limits', {}).get(tf, 10080)
        retained = [candles[p] for p in sorted(candles)[-limit:]]
        result['history'][tf] = retained
        if any(c['period'] not in incoming_periods for c in retained):
            replay.append(tf)
    result['replay_timeframes'] = replay
    result['last_alert'] = dict(old.get('last_alert', {}))
    result['last_alert'].update(new.get('last_alert', {}))
    return result

class HistoryStore:
    def __init__(self, key):
        self.key = key
        self.url = os.getenv('HISTORY_DATABASE_URL', '').strip()
        self.lock = threading.Lock()
        self.status = 'not_configured' if not self.url else 'not_checked'
        self.last_saved_at = None
        self.interval = max(60, int(os.getenv('HISTORY_REMOTE_SAVE_SECONDS', '600')))
        self.next_save = 0

    def connection(self):
        import psycopg
        return psycopg.connect(self.url, connect_timeout=10)

    def prepare(self, connection):
        connection.execute('SET statement_timeout = 15000')
        connection.execute('CREATE TABLE IF NOT EXISTS bubble_checkpoints '
                           '(key TEXT PRIMARY KEY, saved_at DOUBLE PRECISION NOT NULL, payload BYTEA NOT NULL)')

    def save(self, payload, force=False):
        if not self.url:
            return
        if not force and time.monotonic() < self.next_save:
            return
        try:
            compressed = gzip.compress(json.dumps(payload, separators=(',', ':')).encode())
            with self.lock, self.connection() as connection:
                self.prepare(connection)
                connection.execute('SELECT pg_advisory_xact_lock(hashtext(%s))', (self.key,))
                row = connection.execute('SELECT payload FROM bubble_checkpoints WHERE key=%s FOR UPDATE',
                                         (self.key,)).fetchone()
                old = None if row is None else json.loads(gzip.decompress(bytes(row[0])))
                payload = merge_checkpoints(old, payload)
                compressed = gzip.compress(json.dumps(payload, separators=(',', ':')).encode())
                connection.execute('INSERT INTO bubble_checkpoints VALUES (%s,%s,%s) '
                    'ON CONFLICT (key) DO UPDATE SET saved_at=EXCLUDED.saved_at,payload=EXCLUDED.payload '
                    'WHERE bubble_checkpoints.saved_at < EXCLUDED.saved_at',
                    (self.key, payload['saved_at'], compressed))
            self.status = 'saved'
            self.last_saved_at = payload['saved_at']
            self.next_save = time.monotonic() + self.interval
            return payload
        except Exception:
            self.status = 'save_failed'
            log.error('Archivio storico esterno: salvataggio fallito; cache locale conservata')

    def load(self):
        if not self.url:
            return None
        try:
            with self.lock, self.connection() as connection:
                self.prepare(connection)
                row = connection.execute('SELECT payload FROM bubble_checkpoints WHERE key=%s',
                                         (self.key,)).fetchone()
            payload = None if row is None else json.loads(gzip.decompress(bytes(row[0])))
            self.status = 'empty' if payload is None else 'restored'
            self.last_saved_at = None if payload is None else payload['saved_at']
            return payload
        except Exception:
            self.status = 'load_failed'
            log.error('Archivio storico esterno: lettura fallita; tentativo dalla cache locale')
            return None
