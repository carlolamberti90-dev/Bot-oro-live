"""Persistent delivery of visual events. Does not capture TradingView charts."""
import json
import os
import time
import urllib.request
import urllib.error
from visual_bubble import ALERT_TIMEFRAMES

class Outbox:
    def __init__(self, db):
        self.db = db
        db.execute('CREATE TABLE IF NOT EXISTS telegram_outbox (id TEXT PRIMARY KEY, text TEXT NOT NULL, state TEXT NOT NULL, message_id INTEGER)')
        db.commit()

    def enqueue(self, events):
        # Read committed events too: a crash between observation and enqueue
        # must not lose the notification on the next scan.
        for identity, raw in self.db.execute("SELECT id,payload FROM events WHERE state='new_visual_event'").fetchall():
            event = json.loads(raw)
            if event['timeframe'] not in ALERT_TIMEFRAMES:
                continue
            marker = '🟢 LONG' if event['direction'] == 'LONG' else '🔴 SHORT'
            label = 'bubble Pine' if event.get('source') == 'pine' else 'bubble visiva'
            if event.get('pilot_mode'):
                label += ' · PROVA SPERIMENTALE'
            text = f"{marker} — {label}\nOANDA:XAUUSD · {event['timeframe']}\nOsservata: {event['first_observed_at']}\nEvento: {identity}"
            self.db.execute('INSERT OR IGNORE INTO telegram_outbox VALUES (?,?,?,NULL)', (identity,text,'pending'))
        self.db.commit()

    def deliver(self, sender, batch_ids=None):
        # Apply current policy to pending jobs saved by an older version too.
        for identity, raw in self.db.execute("SELECT o.id,e.payload FROM telegram_outbox o JOIN events e ON e.id=o.id WHERE o.state='pending'").fetchall():
            if json.loads(raw)['timeframe'] not in ALERT_TIMEFRAMES:
                self.db.execute("UPDATE telegram_outbox SET state='suppressed' WHERE id=?", (identity,))
        self.db.commit()
        rows = self.db.execute("SELECT id,text FROM telegram_outbox WHERE state='pending' ORDER BY rowid").fetchall()
        if batch_ids is not None:
            rows = [row for row in rows if json.loads(self.db.execute('SELECT payload FROM events WHERE id=?',(row[0],)).fetchone()[0]).get('observation_batch') in batch_ids]
        if rows and sender is telegram_sender:
            # Local configuration failures happen before any network request.
            # Keep jobs pending so fixing configuration can resume delivery.
            telegram_configuration()
        groups={}
        for identity,text in rows:
            row=self.db.execute('SELECT payload FROM events WHERE id=?',(identity,)).fetchone()
            event=json.loads(row[0]) if row else {}
            groups.setdefault(event.get('observation_batch') or identity,[]).append((identity,text))
        for group in groups.values():
            chunks=[];current=[];length=0
            for row in group:
                if current and length+len(row[1])+2>3900:
                    chunks.append(current);current=[];length=0
                current.append(row);length+=len(row[1])+2
            if current:chunks.append(current)
            for chunk in chunks:
                identities=[row[0] for row in chunk]
                text='\n\n'.join(row[1] for row in chunk)
                self.db.executemany("UPDATE telegram_outbox SET state='sending' WHERE id=?",[(i,) for i in identities])
                self.db.commit()
                try:
                    message_id=sender(text)
                    if type(message_id) is not int:raise ValueError('telegram_ack_missing')
                except Exception:
                    self.db.executemany("UPDATE telegram_outbox SET state='unknown' WHERE id=?",[(i,) for i in identities])
                    self.db.commit();continue
                self.db.executemany("UPDATE telegram_outbox SET state='sent',message_id=? WHERE id=?",[(message_id,i) for i in identities])
                self.db.commit()

def telegram_configuration():
    token = os.environ.get('TELEGRAM_BOT_TOKEN', '').strip()
    chat = os.environ.get('TELEGRAM_CHAT_ID', '').strip()
    if not token or not chat:
        raise ValueError('telegram_configuration_missing')
    return token, chat

def telegram_sender(text):
    token, chat = telegram_configuration()
    req = urllib.request.Request(f'https://api.telegram.org/bot{token}/sendMessage',
        data=json.dumps({'chat_id':chat,'text':text}).encode(),
        headers={'Content-Type':'application/json'}, method='POST')
    with urllib.request.urlopen(req, timeout=15) as response:
        result=json.load(response)
    if result.get('ok') is not True:
        raise ValueError('telegram_delivery_not_confirmed')
    return result.get('result', {}).get('message_id')
