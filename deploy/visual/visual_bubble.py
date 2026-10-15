"""Offline visual detector. No browser login, network access or Telegram sends."""
import argparse
import hashlib
import json
import sqlite3
import os
from datetime import datetime
from pathlib import Path

import numpy as np
from PIL import Image
from scipy import ndimage

TIMEFRAMES = {'M1', 'M3', 'M5', 'M15', 'M30', 'M45', 'H1', 'H4', 'D1'}
ALERT_TIMEFRAMES = TIMEFRAMES - {'M1'}

def detect(image, roi):
    """Return coloured round shapes, not a claim of LuxAlgo semantic identity."""
    left, top, right, bottom = roi
    rgb = np.asarray(image.convert('RGB').crop(roi), dtype=np.int16)
    r, g, b = rgb[:, :, 0], rgb[:, :, 1], rgb[:, :, 2]
    masks = {'LONG': (g-r > 30) & (b-r > 15) & (g-b > 5) & (r < 180),
             'SHORT': (r-g > 25) & (r-b > 15) & (g < 200)}
    found = []
    for direction, mask in masks.items():
        for opening,closing,minimum,maximum in ((9,5,18,160),(3,3,8,17)):
            clean=ndimage.binary_opening(mask,structure=np.ones((opening,opening)))
            clean=ndimage.binary_closing(clean,structure=np.ones((closing,closing)))
            labels,_=ndimage.label(clean)
            for label,box in enumerate(ndimage.find_objects(labels),1):
                if box is None:continue
                ys,xs=box
                width,height=xs.stop-xs.start,ys.stop-ys.start
                if not (minimum<=width<=maximum and minimum<=height<=maximum):continue
                if not .72<=width/height<=1.38:continue
                patch=ndimage.binary_fill_holes(labels[box]==label)
                fill=float(patch.mean())
                if not .58<=fill<=.89:continue
                edge=3 if minimum==18 else 1
                corners=[patch[:edge,:edge],patch[:edge,-edge:],patch[-edge:,:edge],patch[-edge:,-edge:]]
                if sum(float(c.mean()) for c in corners)>1.5:continue
                found.append({'direction':direction,'bbox':[left+xs.start,top+ys.start,left+xs.stop,top+ys.stop],
                              'shape_fill':round(fill,3),'classification':'visual_candidate'})
    return sorted(found, key=lambda v:v['bbox'][0])

def validate_frame(meta):
    if meta.get('timeframe') not in TIMEFRAMES:
        raise ValueError('unsupported_timeframe')
    if meta.get('rendered_timeframe') != meta['timeframe']:
        raise ValueError('timeframe_still_loading')
    if meta.get('symbol') != 'OANDA:XAUUSD' or not meta.get('indicator_visible'):
        raise ValueError('symbol_or_indicator_not_verified')
    if not meta.get('captured_at'):
        raise ValueError('capture_time_missing')

class EventStore:
    """Identity uses candle time, NEVER pixel position. First scan seeds history."""
    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.execute('CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY, payload TEXT, state TEXT)')
        self.db.execute('CREATE TABLE IF NOT EXISTS scans (tf TEXT PRIMARY KEY)')
        self.db.execute('CREATE TABLE IF NOT EXISTS scan_candles (tf TEXT PRIMARY KEY,candle TEXT,captured_at TEXT)')
        self.db.commit()

    def observe(self, meta, candidates):
        validate_frame(meta)
        tf = meta['timeframe']
        baseline = self.db.execute('SELECT 1 FROM scans WHERE tf=?',(tf,)).fetchone() is None
        previous=self.db.execute('SELECT candle,captured_at FROM scan_candles WHERE tf=?',(tf,)).fetchone()
        output=[]
        for item in candidates:
            candle=item.get('candle_start')
            if not candle or item.get('association_verified') is not True:
                output.append({**item,'state':'needs_candle_association'})
                continue
            # Multiple labels can belong to one candle. A stable identifier must
            # come from the capture adapter; never derive it from screen pixels.
            bubble_id=item.get('bubble_id')
            if not isinstance(bubble_id,str) or not bubble_id.strip():
                output.append({**item,'state':'needs_bubble_identity'})
                continue
            key='|'.join([meta['symbol'],tf,candle,item['direction'],bubble_id])
            identity=hashlib.sha256(key.encode()).hexdigest()[:24]
            payload={**item,'id':identity,'timeframe':tf,'first_observed_at':meta['captured_at']}
            if meta.get('pilot_mode'):
                payload['pilot_mode']=True
            if meta.get('observation_batch'):
                payload['observation_batch']=meta['observation_batch']
            historical=meta.get('latest_candle_start') and candle!=meta['latest_candle_start']
            # Accept a just-closed candle only if it was demonstrably the latest
            # in the preceding scan, at most 60 seconds ago. Panned older bars
            # remain silent; an unobserved session cannot manufacture events.
            if historical and previous and candle==previous[0] and meta.get('latest_candle_start')!=previous[0]:
                try:
                    elapsed=(datetime.fromisoformat(meta['captured_at'])-datetime.fromisoformat(previous[1])).total_seconds()
                    if 0<=elapsed<=60:
                        historical=False
                        payload['observed_after_rollover']=True
                except (ValueError,TypeError):
                    pass
            state='baseline_existing' if baseline or historical or meta.get('seed_current_observation') else 'new_visual_event'
            cursor=self.db.execute('INSERT OR IGNORE INTO events VALUES (?,?,?)',
                                   (identity,json.dumps(payload),state))
            output.append({**payload,'state':state if cursor.rowcount else 'already_seen'})
        if all(item.get('association_verified') is True and item.get('candle_start') and
               isinstance(item.get('bubble_id'),str) and item['bubble_id'].strip()
               for item in candidates):
            self.db.execute('INSERT OR IGNORE INTO scans VALUES (?)',(tf,))
            if meta.get('latest_candle_start'):
                self.db.execute('INSERT OR REPLACE INTO scan_candles VALUES (?,?,?)',
                                (tf,meta['latest_candle_start'],meta['captured_at']))
        self.db.commit()
        return output

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('image'); p.add_argument('--roi',nargs=4,type=int,required=True)
    p.add_argument('--meta',required=True);p.add_argument('--database',default='visual_events.sqlite')
    p.add_argument('--send-telegram',action='store_true',help='Send verified new visual events; requires Telegram environment variables')
    args=p.parse_args()
    meta=json.loads(Path(args.meta).read_text())
    validate_frame(meta)
    candidates=detect(Image.open(args.image),tuple(args.roi))
    # Sidecar associations must be supplied by a capture adapter reading the data window.
    for candidate in candidates:
        for association in meta.get('associations',[]):
            x=(candidate['bbox'][0]+candidate['bbox'][2])/2
            if abs(x-association['x']) < association.get('tolerance',8):
                candidate.update(candle_start=association['candle_start'],association_verified=True,
                                 bubble_id=association.get('bubble_id'))
    if args.send_telegram and not all(os.environ.get(k,'').strip() for k in ('TELEGRAM_BOT_TOKEN','TELEGRAM_CHAT_ID')):
        p.error('Telegram environment variables are missing')
    store=EventStore(args.database)
    events=store.observe(meta,candidates)
    from telegram_outbox import Outbox,telegram_sender
    outbox=Outbox(store.db)
    outbox.enqueue(events)
    if args.send_telegram:
        outbox.deliver(telegram_sender)
    print(json.dumps(events,indent=2))

if __name__=='__main__':
    main()
