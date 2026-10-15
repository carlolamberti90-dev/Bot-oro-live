"""Continuous capture, visible-candle association and optional Telegram outbox.

Server adapter requires live validation; default configuration disables sends.

Run on the intended browser host with a dedicated, user-authenticated profile.
The conversation browser is not a supported profile source.
"""
import argparse
import io
import json
import signal
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

from PIL import Image
from visual_bubble import TIMEFRAMES, ALERT_TIMEFRAMES, detect, validate_frame, EventStore
from chart_association import INTERVAL_TEXT, DataWindowAssociator
from telegram_outbox import Outbox, telegram_sender
import re

ORDER = ('M1','M3','M5','M15','M30','M45','H1','H4','D1')
CHART_LABELS = {tf: f'Chart for OANDA:XAUUSD, {duration}' for tf, duration in zip(ORDER,
    ('1 minute','3 minutes','5 minutes','15 minutes','30 minutes','45 minutes','1 hour','4 hours','1 day'))}


def validate_config(config):
    charts = config.get('charts', {})
    if set(charts) != TIMEFRAMES:
        raise ValueError('all_nine_timeframes_required')
    if config.get('associate_data_window'):
        from zoneinfo import ZoneInfo
        ZoneInfo(config['chart_timezone'])
    for flag in ('associate_data_window','live_validation_completed','pilot_mode','sequential_tab_mode'):
        if flag in config and type(config[flag]) is not bool:
            raise ValueError('boolean_config_required')
    interval = config.get('scan_interval_seconds', 5)
    requested=config.get('active_timeframes',list(ORDER))
    if not isinstance(requested,list) or len(requested)!=len(set(requested)) or not set(requested)<=TIMEFRAMES or not ALERT_TIMEFRAMES<=set(requested):
        raise ValueError('required_alert_timeframes_missing')
    if not isinstance(interval, (int, float)) or not 1 <= interval <= 60:
        raise ValueError('invalid_scan_interval')
    for tf, chart in charts.items():
        parsed = urlsplit(chart.get('url') or '')
        if parsed.scheme != 'https' or parsed.hostname != 'www.tradingview.com' or not parsed.path.startswith('/chart/') or parsed.username or parsed.password:
            raise ValueError('verified_tradingview_chart_url_required')
        if chart.get('chart_label') != CHART_LABELS[tf]:
            raise ValueError('verified_chart_label_required')
        roi = chart.get('roi')
        if not isinstance(roi, list) or len(roi) != 4 or any(type(n) is not int for n in roi) or not (0 <= roi[0] < roi[2] and 0 <= roi[1] < roi[3]):
            raise ValueError('invalid_roi')


class PlaywrightCapture:
    def __init__(self, config, profile, headed=False):
        validate_config(config)
        from playwright.sync_api import sync_playwright
        self.config = config
        self.driver = sync_playwright().start()
        try:
            self.context = self.driver.chromium.launch_persistent_context(
                str(Path(profile).resolve()), headless=not headed,
                viewport={'width':1280,'height':900}, locale='en-US')
        except Exception:
            self.driver.stop()
            raise
        self.pages = {}
        self.shared_page = None
        for restored in self.context.pages:
            restored.close()

    def capture(self, tf):
        chart = self.config['charts'][tf]
        page = getattr(self,'shared_page',None) if self.config.get('sequential_tab_mode') else self.pages.get(tf)
        if page is None or page.is_closed():
            page = self.context.new_page()
            try:
                page.goto(chart['url'], wait_until='domcontentloaded', timeout=30000)
            except Exception:
                page.close()
                raise
            self.pages[tf] = page
            if self.config.get('sequential_tab_mode'):
                self.shared_page=page
        label=page.get_by_label(chart['chart_label'],exact=True)
        if not label.is_visible():
            page.get_by_role('button',name='Change interval',exact=True).click()
            field=page.get_by_role('textbox')
            field.fill(INTERVAL_TEXT[tf]);field.press('Enter')
        label.wait_for(state='visible',timeout=15000)
        interval=page.get_by_role('button',name='Change interval',exact=True)
        interval.filter(has_text=re.compile('^'+re.escape(INTERVAL_TEXT[tf])+'$')).wait_for(state='visible',timeout=15000)
        legend=page.get_by_role('toolbar').filter(has_text='LuxAlgo - Volumetric Order Flow Structure')
        legend.get_by_role('button',name='Hide',exact=True).wait_for(state='visible',timeout=15000)
        if legend.count()!=1:raise ValueError('indicator_not_verified')
        for name,expected in [('Pivot Length','3'),('Volume Lookback','22'),('Max Recent Blocks','4'),('Base Transparency','88'),('Bubble Sensitivity','2.5')]:
            if legend.get_by_label(name,exact=True).inner_text().strip()!=expected:
                raise ValueError('indicator_parameters_mismatch')
        associator=None;latest=None
        if self.config.get('associate_data_window'):
            associator=DataWindowAssociator(page,self.config['chart_timezone'])
            associator.prepare();latest=associator.latest(tf)
        meta=dict(symbol='OANDA:XAUUSD',timeframe=tf,rendered_timeframe=tf,indicator_visible=True,
                  captured_at=datetime.now(timezone.utc).isoformat(),validation_scope='chart_label_interval_legend_parameters')
        screenshot=page.screenshot(type='png')
        if not label.is_visible() or interval.inner_text().strip()!=INTERVAL_TEXT[tf] or not legend.get_by_role('button',name='Hide',exact=True).is_visible():
            raise ValueError('chart_changed_during_capture')
        if associator:
            candidates=detect(Image.open(io.BytesIO(screenshot)),tuple(chart['roi']))
            meta['associated_candidates']=associator.associate(tf,candidates,chart['roi'])
            meta['latest_candle_start']=latest
            if associator.latest(tf)!=latest or not label.is_visible() or interval.inner_text().strip()!=INTERVAL_TEXT[tf]:
                raise ValueError('chart_changed_during_association')
        return screenshot,meta

    def close(self):
        try:
            self.context.close()
        finally:
            self.driver.stop()


class CaptureAudit:
    def __init__(self, directory, max_frames=180, database=None, sender=None):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        if max_frames < 9:
            raise ValueError('retention_too_small')
        self.max_frames = max_frames
        if sender and not database:raise ValueError('database_required_for_delivery')
        self.store=EventStore(database) if database else None
        self.outbox=Outbox(self.store.db) if self.store else None
        from hybrid_bridge import HybridBridge
        self.hybrid=HybridBridge(self.store.db) if self.store else None
        self.sender=sender
        self.seed_pending=set(ORDER)
        self.status = {tf: {'state':'not_observed','alerts_enabled':tf in ALERT_TIMEFRAMES} for tf in ORDER}

    def scan(self, source, config):
        if self.sender and not (config.get('live_validation_completed') or config.get('pilot_mode')):
            raise ValueError('live_validation_required')
        batch=uuid.uuid4().hex
        if self.hybrid:
            self.hybrid.import_audit(config.get('analytical_audit_file'))
        for tf in ORDER:
            if tf not in config.get('active_timeframes',ORDER):
                self.status[tf]={'state':'disabled','alerts_enabled':False,'notification_ready':False}
                continue
            try:
                raw, meta = source.capture(tf)
                meta['observation_batch']=batch
                validate_frame(meta)
                if meta['timeframe'] != tf:
                    raise ValueError('wrong_capture_timeframe')
                image = Image.open(io.BytesIO(raw)); image.load()
                roi = config['charts'][tf]['roi']
                if roi[2] > image.width or roi[3] > image.height:
                    raise ValueError('roi_outside_image')
                candidates = detect(image, tuple(roi))
                associated=meta.get('associated_candidates')
                if associated is not None:
                    if [(c['bbox'],c['direction']) for c in associated]!=[(c['bbox'],c['direction']) for c in candidates]:
                        raise ValueError('candidate_association_mismatch')
                    candidates=associated
                ready=bool(config.get('live_validation_completed') or config.get('pilot_mode')) and associated is not None and bool(meta.get('latest_candle_start')) and all(c.get('association_verified') is True for c in candidates)
                meta['pilot_mode']=bool(config.get('pilot_mode'))
                meta['seed_current_observation']=tf in self.seed_pending
                events=self.store.observe(meta,candidates) if self.store and ready else []
                if self.store and ready:self.seed_pending.discard(tf)
                name = f'capture_{time.time_ns()}_{tf}_{uuid.uuid4().hex[:8]}'
                image.save(self.directory / f'{name}.png')
                record = {**meta, 'roi':roi, 'candidates':candidates,
                          'alerts_enabled':tf in ALERT_TIMEFRAMES,
                          'notification_ready':ready,'events':events,
                          'association_status':'data_window_associated' if ready else 'live_validation_required'}
                (self.directory / f'{name}.json').write_text(json.dumps(record), encoding='utf-8')
                self.status[tf] = {'state':'captured_candidates_only','captured_at':meta['captured_at'],
                                   'candidate_count':len(candidates),'alerts_enabled':tf in ALERT_TIMEFRAMES,
                                   'notification_ready':ready}
            except Exception as exc:
                # No raw exception text: URLs/credentials can occur in errors.
                self.status[tf] = {'state':'capture_failed','error_type':type(exc).__name__,
                                   'alerts_enabled':tf in ALERT_TIMEFRAMES,'notification_ready':False}
        if self.outbox:
            self.hybrid.reconcile()
            self.outbox.enqueue([])
            if self.sender:self.outbox.deliver(self.sender)
        self._prune()
        temporary = self.directory / 'capture_status.json.tmp'
        delivery=dict(self.store.db.execute('SELECT state,count(*) FROM telegram_outbox GROUP BY state').fetchall()) if self.outbox else {}
        healthy_delivery=not any(delivery.get(state,0) for state in ('unknown','sending','pending'))
        running=bool(self.sender) and healthy_delivery and all(self.status[tf].get('notification_ready') for tf in ALERT_TIMEFRAMES)
        temporary.write_text(json.dumps({'timeframes':self.status,'delivery_states':delivery,'hybrid':self.hybrid.snapshot() if self.hybrid else None,'pilot_alerts_running':running and bool(config.get('pilot_mode')),'visual_alerts_operational':running and bool(config.get('live_validation_completed'))}), encoding='utf-8')
        temporary.replace(self.directory / 'capture_status.json')
        return self.status

    def _prune(self):
        # Only the collector's own paired captures; never arbitrary user files.
        for image in sorted(self.directory.glob('capture_*.png'))[:-self.max_frames]:
            image.with_suffix('.json').unlink(missing_ok=True)
            image.unlink()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    parser.add_argument('--profile', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--headed', action='store_true')
    parser.add_argument('--database')
    parser.add_argument('--send-telegram',action='store_true')
    parser.add_argument('--once',action='store_true')
    args = parser.parse_args()
    import fcntl
    lock_path=Path(args.profile).resolve().parent/'bubble_monitor.lock'
    lock_path.parent.mkdir(parents=True,exist_ok=True)
    lock=lock_path.open('a')
    try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError:parser.error('A monitor is already running for this profile')
    config = json.loads(Path(args.config).read_text())
    validate_config(config)
    if args.send_telegram and (not args.database or not (config.get('live_validation_completed') or config.get('pilot_mode')) or not config.get('associate_data_window')):
        parser.error('Live validation, association and persistent database required for Telegram')
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    source = PlaywrightCapture(config, args.profile, args.headed)
    audit=CaptureAudit(args.output,database=args.database,sender=telegram_sender if args.send_telegram else None)
    try:
        while not stop.is_set():
            audit.scan(source, config)
            if args.once:break
            stop.wait(config.get('scan_interval_seconds', 5))
    finally:
        source.close()
        if audit.store:audit.store.db.close()


if __name__ == '__main__':
    main()
