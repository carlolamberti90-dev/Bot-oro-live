"""Authenticated setup browser and monitor launcher; no browser profile export."""
import base64
import hmac
import hashlib
import json
import os
import queue
import secrets
import subprocess
import sys
import threading
import time
from concurrent.futures import Future
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from http.cookies import SimpleCookie
from urllib.parse import parse_qs

ROOT = Path(__file__).resolve().parent
DATA = Path(os.getenv('MONITOR_DATA_DIR', '/tmp/bubble-monitor'))
SESSION_KEY = secrets.token_bytes(32)
LOGIN_PAGE = b'''<!doctype html><html lang="it"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Accesso monitor oro</title><main><h1>Accesso monitor oro</h1><p>Inserisci le credenziali del monitor. Non sono quelle di TradingView.</p><form action="/login" method="post"><p><label>Utente <input name="username" autocomplete="username" required></label></p><p><label>Password <input name="password" type="password" autocomplete="current-password" required></label></p><button type="submit">Entra nel monitor</button></form><p>Se l'accesso non riesce, controlla di aver copiato tutta la password, rispettando maiuscole e minuscole.</p></main></html>'''


def session_cookie():
    payload = str(int(time.time()) + 8 * 3600) + '.' + secrets.token_hex(16)
    signature = hmac.new(SESSION_KEY, payload.encode(), hashlib.sha256).hexdigest()
    return payload + '.' + signature


def valid_session(raw):
    try:
        cookie = SimpleCookie(); cookie.load(raw or '')
        value = cookie['monitor_session'].value
        expiry, nonce, signature = value.split('.')
        payload = expiry + '.' + nonce
        expected = hmac.new(SESSION_KEY, payload.encode(), hashlib.sha256).hexdigest()
        return int(expiry) > time.time() and hmac.compare_digest(signature, expected)
    except (KeyError, ValueError, TypeError):
        return False


def same_origin(headers):
    external = os.getenv('RENDER_EXTERNAL_URL','').rstrip('/')
    expected = external or 'https://' + headers.get('Host','')
    return headers.get('Origin','') == expected


def login_csrf_valid(headers, fields):
    try:
        cookies=SimpleCookie();cookies.load(headers.get('Cookie',''))
        cookie=cookies['__Host-monitor_csrf'].value
        token=fields.get('csrf',[''])[0]
        # iOS in-app views can omit Origin or send null. A random double-submit
        # token still binds submission to a form read from this origin.
        origin=headers.get('Origin','')
        origin_ok=origin in ('','null') or same_origin(headers)
        return origin_ok and len(cookie) >= 40 and hmac.compare_digest(cookie.encode(),token.encode())
    except (KeyError,ValueError,TypeError):
        return False


def authorized(header, password):
    if len(password) < 24:
        return False
    expected = 'Basic ' + base64.b64encode(('carlo:' + password).encode()).decode()
    return hmac.compare_digest(header or '', expected)


class Controller:
    def __init__(self):
        DATA.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.jobs = queue.Queue(maxsize=16)
        self.process = None
        self.context = None
        self.page = None
        self.driver = None
        self.error = None
        self.started = None
        threading.Thread(target=self.worker, daemon=True).start()

    def worker(self):
        while True:
            action, payload, future = self.jobs.get()
            if not future.set_running_or_notify_cancel():
                continue
            try:
                future.set_result(self.execute(action, payload))
            except Exception as exc:
                self.error = type(exc).__name__
                future.set_exception(RuntimeError(self.error))

    def call(self, action, payload=None):
        future = Future()
        self.jobs.put_nowait((action, payload or {}, future))
        try:
            return future.result(timeout=45)
        except TimeoutError:
            future.cancel()
            raise

    def close_browser(self):
        if self.context:
            self.context.close()
        if self.driver:
            self.driver.stop()
        self.context = self.driver = self.page = None

    def execute(self, action, payload):
        running = self.process is not None and self.process.poll() is None
        if action == 'status':
            status = None
            path = DATA / 'captures' / 'capture_status.json'
            if path.exists():
                status = json.loads(path.read_text())
                status['age_seconds'] = round(time.time() - path.stat().st_mtime)
            return {'monitor_running': running, 'browser_open': self.page is not None,
                    'telegram_configured': bool(os.getenv('TELEGRAM_BOT_TOKEN') and os.getenv('TELEGRAM_CHAT_ID')),
                    'capture_status': status, 'error_type': self.error,
                    'storage': 'ephemeral; login and deduplication reset on host restart',
                    'live_validation_completed': False}
        if action == 'stop':
            if running:
                self.process.terminate()
                try:
                    self.process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=5)
            self.process = None
            return {'stopped': True}
        if running:
            raise ValueError('stop_monitor_before_login')
        if action == 'start':
            self.close_browser()
            # Old captures must never present a previous run as ready.
            (DATA / 'captures' / 'capture_status.json').unlink(missing_ok=True)
            args = [sys.executable, str(ROOT / 'chart_capture.py'), '--config',
                    str(ROOT / 'monitor.example.json'), '--profile', str(DATA / 'profile'),
                    '--output', str(DATA / 'captures'), '--database', str(DATA / 'events.sqlite')]
            if payload.get('send_telegram') is True:
                if not os.getenv('TELEGRAM_BOT_TOKEN') or not os.getenv('TELEGRAM_CHAT_ID'):
                    raise ValueError('telegram_credentials_missing')
                args.append('--send-telegram')
            self.error = None
            self.started = time.time()
            self.process = subprocess.Popen(args, cwd=ROOT)
            return {'started': True, 'validated': False}
        if action == 'open':
            if not self.context:
                from playwright.sync_api import sync_playwright
                self.driver = sync_playwright().start()
                self.context = self.driver.chromium.launch_persistent_context(
                    str(DATA / 'profile'), headless=True,
                    viewport={'width':1280,'height':900}, locale='en-US')
                self.page = self.context.pages[0] if self.context.pages else self.context.new_page()
            config = json.loads((ROOT / 'monitor.example.json').read_text())
            self.page.goto(config['charts']['M45']['url'], wait_until='domcontentloaded', timeout=30000)
            return {'opened': True}
        if not self.page:
            raise ValueError('open_browser_first')
        # A login popup becomes the active view, without exporting cookies.
        pages = [page for page in self.context.pages if not page.is_closed()]
        if pages:
            self.page = pages[-1]
        if action == 'frame':
            return self.page.screenshot(type='jpeg', quality=75)
        if action == 'click':
            x, y = float(payload['x']), float(payload['y'])
            if not (0 <= x < 1280 and 0 <= y < 900):
                raise ValueError('coordinates_out_of_bounds')
            self.page.mouse.click(x,y)
        elif action == 'text':
            self.page.keyboard.insert_text(str(payload['text'])[:4096])
        elif action == 'key':
            if payload.get('key') not in ('Enter','Tab','Escape','Backspace','ControlOrMeta+A'):
                raise ValueError('unsupported_key')
            self.page.keyboard.press(payload['key'])
        elif action == 'scroll':
            self.page.mouse.wheel(0, max(-900,min(900,int(payload['dy']))))
        else:
            raise ValueError('unknown_action')
        return {'ok':True}


class Handler(BaseHTTPRequestHandler):
    controller = None
    def log_message(self, *args):
        pass  # No URLs, Authorization headers or typed login data in logs.

    def reply(self, code, body, kind='application/json', extra_headers=None):
        if isinstance(body, dict):
            body = json.dumps(body).encode()
        self.send_response(code)
        self.send_header('Content-Type',kind)
        self.send_header('Cache-Control','no-store')
        self.send_header('X-Content-Type-Options','nosniff')
        self.send_header('X-Frame-Options','DENY')
        self.send_header('Referrer-Policy','no-referrer')
        self.send_header('Content-Security-Policy',"default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' blob:; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
        for key,value in (extra_headers or {}).items():
            self.send_header(key,value)
        self.send_header('Content-Length',str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def access(self):
        if valid_session(self.headers.get('Cookie')) or authorized(self.headers.get('Authorization'),os.getenv('SETUP_PASSWORD','')):
            return True
        self.reply(401,{'error':'authentication_required'})
        return False

    def show_login(self, code=200, incorrect=False):
        token=secrets.token_urlsafe(32)
        page=LOGIN_PAGE.replace(b'<p><label>Utente',
            ('<input type="hidden" name="csrf" value="'+token+'"><p><label>Utente').encode(),1)
        if incorrect:
            page=page.replace(b'<h1>',b'<p>Credenziali non corrette. Riprova.</p><h1>',1)
        return self.reply(code,page,'text/html; charset=utf-8',extra_headers={
            'Set-Cookie':'__Host-monitor_csrf='+token+'; Path=/; Secure; HttpOnly; SameSite=Lax; Max-Age=900'})

    def do_GET(self):
        if self.path == '/health':
            return self.reply(200,{'service':'setup_available','monitor_operational':False})
        if self.path == '/login' or (self.path == '/' and not valid_session(self.headers.get('Cookie')) and not authorized(self.headers.get('Authorization'),os.getenv('SETUP_PASSWORD',''))):
            return self.show_login()
        if not self.access():
            return
        try:
            if self.path == '/status':
                return self.reply(200,self.controller.call('status'))
            if self.path == '/frame':
                return self.reply(200,self.controller.call('frame'),'image/jpeg')
            files={'/':('setup.html','text/html; charset=utf-8'),'/setup.js':('setup.js','application/javascript'),'/setup.css':('setup.css','text/css')}
            if self.path in files:
                name,kind=files[self.path]
                return self.reply(200,(ROOT/name).read_bytes(),kind)
            self.reply(404,{'error':'not_found'})
        except Exception:
            self.reply(409,{'error':'action_unavailable; inspect status'})

    def do_POST(self):
        if self.path == '/login':
            try:
                length=int(self.headers.get('Content-Length','0'))
                if not 0 < length <= 10000:
                    return self.reply(413,{'error':'invalid_body_size'})
                fields=parse_qs(self.rfile.read(length).decode())
                if not login_csrf_valid(self.headers,fields):
                    return self.show_login(403)
                username=fields.get('username',[''])[0].strip().casefold()
                password=fields.get('password',[''])[0].strip()
                expected=os.getenv('SETUP_PASSWORD','')
                if username != 'carlo' or len(expected) < 24 or not hmac.compare_digest(password.encode(),expected.encode()):
                    return self.show_login(401,incorrect=True)
                return self.reply(303,b'',extra_headers={'Location':'/','Set-Cookie':'monitor_session='+session_cookie()+'; Path=/; Secure; HttpOnly; SameSite=Strict; Max-Age=28800'})
            except (ValueError,UnicodeError):
                return self.reply(400,{'error':'invalid_login'})
        if not self.access():
            return
        # Same-origin fetch only, including protection against authenticated CSRF.
        if not same_origin(self.headers) or self.headers.get('X-Monitor-Action') != '1':
            return self.reply(403,{'error':'origin_rejected'})
        try:
            length=int(self.headers.get('Content-Length','0'))
            if not 0 < length <= 20000:
                return self.reply(413,{'error':'invalid_body_size'})
            payload=json.loads(self.rfile.read(length))
            if self.path != '/action' or not isinstance(payload,dict):
                return self.reply(400,{'error':'invalid_action'})
            action=payload.pop('action',None)
            if action not in ('open','start','stop','click','text','key','scroll'):
                return self.reply(400,{'error':'invalid_action'})
            self.reply(200,self.controller.call(action,payload))
        except Exception:
            self.reply(409,{'error':'action_failed; inspect status'})


if __name__ == '__main__':
    if len(os.getenv('SETUP_PASSWORD','')) < 24:
        raise SystemExit('SETUP_PASSWORD must contain at least 24 characters')
    Handler.controller=Controller()
    ThreadingHTTPServer(('0.0.0.0',int(os.getenv('PORT','10000'))),Handler).serve_forever()
