"""Analytical audit correlation; only the visual EventStore creates alerts.

Confirmation means same timeframe/direction/bar and near observation time,
not proof of an identical LuxAlgo label. D1 requires an explicit candle ID.
"""
import json
import math
from datetime import datetime
from pathlib import Path

SECONDS = {'M3':180,'M5':300,'M15':900,'M30':1800,'M45':2700,'H1':3600,'H4':14400}

def instant(value):
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if parsed.tzinfo is None:
            raise ValueError('timezone_required')
        value = parsed.timestamp()
    if isinstance(value, bool) or not isinstance(value, (int,float)) or not math.isfinite(value):
        raise ValueError('invalid_timestamp')
    return float(value)

class HybridBridge:
    def __init__(self, db):
        self.db = db
        db.execute('CREATE TABLE IF NOT EXISTS analytical_candidates (id TEXT PRIMARY KEY, payload TEXT NOT NULL, detected REAL NOT NULL)')
        db.execute('CREATE TABLE IF NOT EXISTS hybrid_comparisons (visual_id TEXT PRIMARY KEY, status TEXT NOT NULL, analytical_ids TEXT NOT NULL)')
        db.commit()
        self.import_status = 'not_configured'

    def ingest(self, record):
        # Trial messages, delivery records and other instruments cannot confirm.
        if record.get('type') != 'candidate' or record.get('mode') not in ('luxalgo_reconstruction','luxalgo_shadow'):
            return False
        if record.get('symbol') not in (None,'OANDA:XAUUSD','OANDA:XAU_USD'):
            return False
        if record.get('tf') not in (*SECONDS,'D1') or record.get('direction') not in ('LONG','SHORT'):
            return False
        identity = record.get('event_id')
        if not isinstance(identity,str) or not identity:
            return False
        try:
            detected = instant(record['detected_at'])
            instant(record['feed_timestamp'])
        except (KeyError,ValueError,TypeError,OverflowError):
            return False
        self.db.execute('INSERT OR IGNORE INTO analytical_candidates VALUES (?,?,?)',
                        (identity,json.dumps(record),detected))
        self.db.commit()
        return True

    def import_audit(self, path):
        # Bounded tail; incomplete concurrent writes are ignored until next scan.
        if not path:
            self.import_status = 'not_configured'
            return
        try:
            with Path(path).open('rb') as stream:
                stream.seek(0,2); size=stream.tell(); offset=max(0,size-5*1024*1024)
                stream.seek(offset)
                if offset: stream.readline()
                data=stream.read(5*1024*1024)
            for line in data.splitlines(keepends=True):
                if not line.endswith(b'\n'): continue
                try:
                    record=json.loads(line)
                    if isinstance(record,dict): self.ingest(record)
                except (ValueError,TypeError): continue
            self.import_status = 'imported'
        except OSError:
            self.import_status = 'unavailable'

    def reconcile(self):
        for visual_id,raw in self.db.execute("SELECT id,payload FROM events WHERE state='new_visual_event'").fetchall():
            event=json.loads(raw); matched=[]
            try: observed=instant(event['first_observed_at'])
            except (KeyError,ValueError,TypeError): continue
            rows=self.db.execute('SELECT id,payload FROM analytical_candidates WHERE detected BETWEEN ? AND ?',
                                 (observed-300,observed+300)).fetchall()
            for identity,candidate_raw in rows:
                candidate=json.loads(candidate_raw)
                if candidate['tf']!=event['timeframe'] or candidate['direction']!=event['direction']: continue
                explicit=candidate.get('candle_start')
                if explicit is not None:
                    same_bar=explicit==event['candle_start']
                elif event['timeframe'] in SECONDS:
                    try:
                        start=instant(event['candle_start']); feed=instant(candidate['feed_timestamp'])
                        same_bar=start<=feed<start+SECONDS[event['timeframe']]
                    except (ValueError,TypeError): same_bar=False
                else: same_bar=False
                if same_bar: matched.append(identity)
            status='analytical_support' if matched else 'visual_only'
            self.db.execute('INSERT OR REPLACE INTO hybrid_comparisons VALUES (?,?,?)',
                            (visual_id,status,json.dumps(sorted(matched))))
        self.db.commit()

    def snapshot(self):
        return {'analytical_import':self.import_status,
                'comparison_counts':dict(self.db.execute('SELECT status,count(*) FROM hybrid_comparisons GROUP BY status')),
                'alert_authority':'visual','analytical_confirmation_required':False}
