"""Authenticated setup browser and monitor launcher; no browser profile export."""
import base64
import hmac
import hashlib
import json
import logging
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
SESSION_TTL = 8 * 3600
REMEMBER_TTL = 30 * 24 * 3600


def session_key():
    # Stable across host replacement; rotating the existing password revokes sessions.
    password = os.getenv('SETUP_PASSWORD', '')
    if len(password) < 24:
        return None
    return hmac.new(password.encode(), b'bubble-monitor/session-signing/v1', hashlib.sha256).digest()
LOG = logging.getLogger('bubble_monitor')
MOBILE_PAGE = b'''<!doctype html><html lang="it"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Accesso TradingView</title><link rel="stylesheet" href="/signin.css"><main><h1>Accedi a TradingView</h1><p>Accesso diretto sul server del monitor.</p><form id="signin"><label for="email">Email TradingView</label><input id="email" type="email" autocomplete="username" value="carlolamberti90@gmail.com" required><label for="password">Password TradingView</label><input id="password" type="password" autocomplete="current-password" required><button id="submit" type="submit">Accedi a TradingView</button></form><p id="result" role="status">Inserisci la password TradingView, non quella di Google o del monitor.</p><p>Il monitor non salva la password. Verifica il risultato prima di avviare gli alert.</p><a href="/">Torna al monitor</a></main><script src="/signin.js" defer></script></html>'''
MOBILE_CSS = b'''*{box-sizing:border-box}body{margin:0;background:#f3f6f8;color:#172c35;font:18px system-ui,sans-serif}main{max-width:480px;margin:auto;padding:28px 20px}h1{font-size:28px}label{display:block;margin:22px 0 8px;font-weight:600}input,button{width:100%;font:inherit;padding:15px;border:1px solid #a7b7c0;border-radius:12px}input{background:white}button{margin-top:24px;background:#1264e7;color:white;border:0;font-weight:600}button:disabled{opacity:.6}p{line-height:1.5}a{display:block;padding:16px 0;color:#064fbf}'''
MOBILE_JS = b'''const form=document.getElementById('signin'),result=document.getElementById('result'),button=document.getElementById('submit'),password=document.getElementById('password');form.addEventListener('submit',async event=>{event.preventDefault();button.disabled=true;result.textContent='Accesso in corso. Attendi senza premere di nuovo.';let body=JSON.stringify({action:'signin',email:document.getElementById('email').value.trim(),password:password.value});password.value='';try{const response=await fetch('/action',{method:'POST',headers:{'Content-Type':'application/json','X-Monitor-Action':'1'},body});body=null;const data=await response.json();if(!response.ok)throw Error('Accesso non completato. Torna al monitor per controllare la schermata.');result.textContent=data.message;}catch(error){result.textContent=error.message;}finally{body=null;button.disabled=false;}});'''
LOGIN_PAGE = b'''<!doctype html><html lang="it"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Accesso monitor oro</title><main><h1>Accesso monitor oro</h1><p>Inserisci le credenziali del monitor. Non sono quelle di TradingView.</p><form action="/login" method="post"><p><label>Utente <input name="username" autocomplete="username" required></label></p><p><label>Password <input name="password" type="password" autocomplete="current-password" required></label></p><p><label><input name="remember" type="checkbox" value="1"> Ricordami su questo dispositivo per 30 giorni</label></p><button type="submit">Entra nel monitor</button></form><p>Se l'accesso non riesce, controlla di aver copiato tutta la password, rispettando maiuscole e minuscole.</p></main></html>'''


def session_cookie(remember=False):
    key = session_key()
    if key is None:
        raise ValueError('session_key_unavailable')
    payload = str(int(time.time()) + (REMEMBER_TTL if remember else SESSION_TTL)) + '.' + secrets.token_hex(16)
    signature = hmac.new(key, payload.encode(), hashlib.sha256).hexdigest()
    return payload + '.' + signature


def valid_session(raw):
    try:
        key = session_key()
        if key is None:
            return False
        cookie = SimpleCookie(); cookie.load(raw or '')
        value = cookie['monitor_session'].value
        expiry, nonce, signature = value.split('.')
        payload = expiry + '.' + nonce
        expected = hmac.new(key, payload.encode(), hashlib.sha256).hexdigest()
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
        self.desired = None
        self.restart_attempts = 0
        self.retry_at = 0
        self.intent_path = DATA / 'monitor_intent.json'
        try:
            intent = json.loads(self.intent_path.read_text())
            if intent.get('enabled') is True and type(intent.get('send_telegram')) is bool:
                self.desired = {'send_telegram': intent['send_telegram']}
        except FileNotFoundError:
            pass
        except (ValueError, OSError):
            self.error = 'resume_state_unavailable'
        LOG.info('controller_started resume_requested=%s', self.desired is not None)
        threading.Thread(target=self.worker, daemon=True).start()

    def worker(self):
        while True:
            try:
                action, payload, future = self.jobs.get(timeout=1)
            except queue.Empty:
                self.supervise()
                continue
            if not future.set_running_or_notify_cancel():
                continue
            try:
                future.set_result(self.execute(action, payload))
            except Exception as exc:
                self.error = type(exc).__name__
                LOG.error('action_failed action=%s error_type=%s', action, self.error)
                future.set_exception(RuntimeError(self.error))

    def save_intent(self, enabled, send_telegram=False):
        temporary = self.intent_path.with_suffix('.tmp')
        temporary.write_text(json.dumps({'enabled': enabled, 'send_telegram': send_telegram}))
        temporary.chmod(0o600)
        temporary.replace(self.intent_path)

    def supervise(self):
        if self.desired is None or (self.process is not None and self.process.poll() is None):
            return
        if self.restart_attempts >= 3 or time.monotonic() < self.retry_at:
            return
        self.restart_attempts += 1
        self.retry_at = time.monotonic() + 30 * (2 ** (self.restart_attempts - 1))
        # Resume only an explicitly started monitor, never enable notifications
        # merely because the web service was deployed or opened.
        try:
            self.launch(self.desired)
        except Exception as exc:
            self.error = type(exc).__name__
            LOG.error('monitor_resume_failed error_type=%s attempt=%s', self.error, self.restart_attempts)

    def launch(self, payload):
        send = payload.get('send_telegram') is True
        if send and not (os.getenv('TELEGRAM_BOT_TOKEN') and os.getenv('TELEGRAM_CHAT_ID')):
            raise ValueError('telegram_credentials_missing')
        self.close_browser()
        (DATA / 'captures' / 'capture_status.json').unlink(missing_ok=True)
        args = [sys.executable, str(ROOT / 'chart_capture.py'), '--config',
                str(ROOT / 'monitor.example.json'), '--profile', str(DATA / 'profile'),
                '--output', str(DATA / 'captures'), '--database', str(DATA / 'events.sqlite')]
        if send:
            args.append('--send-telegram')
        self.process = subprocess.Popen(args, cwd=ROOT)
        self.started = time.time()
        self.error = None
        LOG.info('monitor_started telegram_enabled=%s attempt=%s', send, self.restart_attempts)

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
                try:
                    status = json.loads(path.read_text())
                    status['age_seconds'] = round(time.time() - path.stat().st_mtime)
                except (ValueError, OSError):
                    self.error = 'capture_status_unavailable'
            fresh = bool(running and status and status.get('age_seconds', 601) <= 600)
            return {'monitor_running': running, 'browser_open': self.page is not None,
                    'telegram_configured': bool(os.getenv('TELEGRAM_BOT_TOKEN') and os.getenv('TELEGRAM_CHAT_ID')),
                    'capture_status': status, 'error_type': self.error,
                    'monitor_exit_code': self.process.poll() if self.process is not None else None,
                    'resume_requested': self.desired is not None,
                    'restart_attempts': self.restart_attempts,
                    'capture_status_fresh': fresh,
                    'pilot_alerts_running': bool(fresh and status.get('pilot_alerts_running')),
                    'visual_alerts_operational': bool(fresh and status.get('visual_alerts_operational')),
                    'storage': 'ephemeral; login and deduplication reset on host restart',
                    'live_validation_completed': False}
        if action == 'stop':
            self.save_intent(False)
            self.desired = None
            self.restart_attempts = 0
            if running:
                self.process.terminate()
                try:
                    self.process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=5)
            self.process = None
            LOG.info('monitor_stopped')
            return {'stopped': True}
        if running:
            raise ValueError('stop_monitor_before_login')
        if action == 'start':
            self.launch(payload)
            self.save_intent(True, payload.get('send_telegram') is True)
            self.desired = {'send_telegram': payload.get('send_telegram') is True}
            self.restart_attempts = 0
            self.retry_at = time.monotonic() + 30
            return {'started': True, 'validated': False}
        if action == 'signin':
            # Values come directly from the authenticated user's HTTPS form.
            # Never write credentials to disk, configuration, status, or logs.
            email = str(payload.pop('email', '')).strip()
            password = str(payload.pop('password', ''))
            try:
                if not email or not password or len(email) > 320 or len(password) > 4096:
                    raise ValueError('invalid_login_fields')
                if not self.context or not self.page or self.page.is_closed():
                    self.close_browser()
                    from playwright.sync_api import sync_playwright
                    self.driver = sync_playwright().start()
                    self.context = self.driver.chromium.launch_persistent_context(
                        str(DATA / 'profile'), headless=True,
                        viewport={'width':1024,'height':768}, locale='en-US')
                    self.page = self.context.pages[0] if self.context.pages else self.context.new_page()
                # A slow background resource must not discard a usable form.
                # Reuse the email form if it has already finished loading.
                secret = self.page.locator('input[type="password"]').filter(visible=True)
                if secret.count() != 1:
                    from playwright.sync_api import TimeoutError as BrowserTimeout
                    try:
                        self.page.goto('https://www.tradingview.com/accounts/signin/',
                                       wait_until='commit', timeout=15000)
                    except BrowserTimeout:
                        return {'message':'TradingView sta caricando lentamente. Nessun dato inserito. Torna al monitor e verifica la pagina prima di riprovare.'}
                if self.page.get_by_text('Verify you are human', exact=False).count():
                    return {'message':'TradingView richiede una verifica umana. Torna al monitor; non ripetere la password.'}
                email_option = self.page.get_by_text('Email', exact=True)
                try:
                    self.page.locator('input[type="password"]').filter(visible=True).wait_for(timeout=8000)
                except Exception:
                    pass  # Some layouts show the Email method before the form.
                if email_option.count() == 1 and email_option.is_visible():
                    email_option.click(timeout=5000)
                username = self.page.locator('input[name="id_username"], input[name="username"], input[autocomplete="username"], input[type="email"]').filter(visible=True)
                secret = self.page.locator('input[type="password"]').filter(visible=True)
                try:
                    secret.wait_for(timeout=8000)
                except Exception:
                    return {'message':'Modulo email ancora non disponibile. Nessun dato inserito. Torna al monitor per controllare.'}
                if username.count() != 1 or secret.count() != 1:
                    return {'message':'Modulo TradingView non riconosciuto. Torna al monitor; nessuna password inserita.'}
                username.fill(email, timeout=5000)
                secret.fill(password, timeout=5000)
                submit = self.page.get_by_role('button', name='Sign in', exact=True)
                if submit.count() != 1:
                    secret.fill('')
                    return {'message':'Pulsante di accesso non riconosciuto. Torna al monitor.'}
                # TradingView disables submission while a CAPTCHA is pending.
                # Do not wait for a disabled button or retry credentials.
                if not submit.is_enabled():
                    self.error = None
                    return {'verification_required': True, 'message':'TradingView richiede una verifica di sicurezza prima di accedere. Email e password sono gia inserite: torna al monitor, premi Aggiorna immagine e completa la verifica. Non reinserire la password.'}
                submit.click(timeout=5000)
                return {'message':'Dati inviati a TradingView. Torna al monitor e premi Aggiorna immagine per verificare accesso o eventuale codice. Gli alert non sono ancora attivi.'}
            finally:
                password = None
                email = None
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
            if self.path == '/signin':
                return self.reply(200,MOBILE_PAGE,'text/html; charset=utf-8')
            if self.path == '/signin.css':
                return self.reply(200,MOBILE_CSS,'text/css')
            if self.path == '/signin.js':
                return self.reply(200,MOBILE_JS,'application/javascript')
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
                remember = fields.get('remember', [''])[0] == '1'
                ttl = REMEMBER_TTL if remember else SESSION_TTL
                return self.reply(303,b'',extra_headers={'Location':'/','Set-Cookie':'monitor_session='+session_cookie(remember)+'; Path=/; Secure; HttpOnly; SameSite=Strict; Max-Age='+str(ttl)})
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
            if action not in ('open','start','stop','click','text','key','scroll','signin'):
                return self.reply(400,{'error':'invalid_action'})
            self.reply(200,self.controller.call(action,payload))
        except Exception:
            self.reply(409,{'error':'action_failed; inspect status'})


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    if len(os.getenv('SETUP_PASSWORD','')) < 24:
        raise SystemExit('SETUP_PASSWORD must contain at least 24 characters')
    Handler.controller=Controller()
    LOG.info('setup_server_listening')
    ThreadingHTTPServer(('0.0.0.0',int(os.getenv('PORT','10000'))),Handler).serve_forever()


