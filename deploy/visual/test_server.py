import base64
import http.client
import os
import re
import threading
import json
import tempfile
import time
from pathlib import Path
import unittest
import urllib.request
import urllib.error
from http.server import ThreadingHTTPServer
from unittest.mock import patch, Mock
from server import Controller, Handler, authorized, valid_session, login_csrf_valid, same_origin

class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.data=Path(self.tmp.name)
        self.env=patch('server.DATA', self.data);self.env.start()
        self.c=Controller.__new__(Controller)
        self.c.process=None;self.c.page=None;self.c.context=None;self.c.driver=None
        self.c.error=None;self.c.started=None;self.c.desired=None
        self.c.restart_attempts=0;self.c.retry_at=0
        self.c.intent_path=self.data/'monitor_intent.json'
    def tearDown(self):
        self.env.stop();self.tmp.cleanup()
    def test_deploy_alone_never_starts_monitor(self):
        with patch.object(self.c,'launch') as launch:
            self.c.supervise();launch.assert_not_called()
    def test_crashed_monitor_retries_with_bounded_backoff(self):
        self.c.desired={'send_telegram':False}
        with patch.object(self.c,'launch',side_effect=RuntimeError) as launch:
            self.c.supervise();self.assertEqual(launch.call_count,1)
            self.c.supervise();self.assertEqual(launch.call_count,1)
            for _ in range(4):
                self.c.retry_at=0;self.c.supervise()
            self.assertEqual(launch.call_count,3)
    def test_stop_cancels_resume_and_saves_disabled_intent(self):
        self.c.desired={'send_telegram':True}
        self.c.execute('stop',{})
        self.assertIsNone(self.c.desired)
        self.assertFalse(json.loads(self.c.intent_path.read_text())['enabled'])
    def test_old_capture_cannot_claim_operational(self):
        directory=self.data/'captures';directory.mkdir()
        p=directory/'capture_status.json'
        p.write_text(json.dumps({'pilot_alerts_running':True,'visual_alerts_operational':True}))
        os.utime(p,(time.time()-700,time.time()-700))
        self.c.process=Mock();self.c.process.poll.return_value=None
        status=self.c.execute('status',{})
        self.assertFalse(status['capture_status_fresh'])
        self.assertFalse(status['visual_alerts_operational'])
        self.assertFalse(status['pilot_alerts_running'])
    def test_missing_telegram_credentials_does_not_launch_process(self):
        with patch.dict(os.environ,{},clear=True), patch('server.subprocess.Popen') as process:
            with self.assertRaises(ValueError):self.c.launch({'send_telegram':True})
            process.assert_not_called()
    def test_corrupt_capture_status_does_not_hide_process_status(self):
        directory=self.data/'captures';directory.mkdir()
        (directory/'capture_status.json').write_text('{')
        status=self.c.execute('status',{})
        self.assertEqual(status['error_type'],'capture_status_unavailable')
        self.assertFalse(status['visual_alerts_operational'])

class Fake:
    def __init__(self):self.calls=[]
    def call(self,action,payload=None):
        self.calls.append(action)
        return {'monitor_running':False}

class SecurityTests(unittest.TestCase):
    def setUp(self):
        self.password='test-only-password-32-characters!!'
        self.env=patch.dict(os.environ,{'SETUP_PASSWORD':self.password});self.env.start()
        self.fake=Fake();Handler.controller=self.fake
        self.server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        threading.Thread(target=self.server.serve_forever,daemon=True).start()
        self.url='http://127.0.0.1:'+str(self.server.server_port)
        self.auth='Basic '+base64.b64encode(('carlo:'+self.password).encode()).decode()
    def tearDown(self):
        self.server.shutdown();self.server.server_close();self.env.stop()
    def request(self,path,headers=None,data=None):
        req=urllib.request.Request(self.url+path,data=data,headers=headers or {})
        try:return urllib.request.urlopen(req)
        except urllib.error.HTTPError as e:return e
    def test_private_frames_and_status_require_authentication(self):
        for path in ('/frame','/status','/setup.js'):
            with self.request(path) as r:self.assertEqual(r.status,401)
        self.assertEqual(self.fake.calls,[])
    def test_login_form_is_public_and_has_no_native_challenge(self):
        with self.request('/') as r:
            self.assertEqual(r.status,200)
            self.assertIn(b'Entra nel monitor',r.read())
            self.assertIsNone(r.headers.get('WWW-Authenticate'))
    def login(self,body,origin=None):
        conn=http.client.HTTPConnection('127.0.0.1',self.server.server_port)
        host='127.0.0.1:'+str(self.server.server_port)
        with self.request('/') as page:
            cookie=page.headers['Set-Cookie'].split(';')[0]
            token=re.search(b'name="csrf" value="([^"]+)"',page.read()).group(1).decode()
        conn.request('POST','/login',body+'&csrf='+token,{'Origin':origin or 'https://'+host,'Cookie':cookie,'Content-Type':'application/x-www-form-urlencoded'})
        return conn,conn.getresponse()
    def test_form_login_sets_secure_session_and_authorizes_status(self):
        conn,r=self.login('username=Carlo&password='+self.password)
        self.assertEqual(r.status,303)
        cookie=r.getheader('Set-Cookie');r.read();conn.close()
        for flag in ('Secure','HttpOnly','SameSite=Strict'):self.assertIn(flag,cookie)
        self.assertTrue(valid_session(cookie.split(';')[0]))
        with self.request('/status',{'Cookie':cookie.split(';')[0]}) as s:self.assertEqual(s.status,200)
        self.assertFalse(valid_session(cookie.replace('monitor_session=','monitor_session=0')))
    def test_wrong_password_and_cross_origin_do_not_set_cookie(self):
        for body,origin,code in [('username=carlo&password=wrong',None,401),('username=carlo&password='+self.password,'https://evil.example',403)]:
            conn,r=self.login(body,origin)
            self.assertEqual(r.status,code);self.assertNotIn('monitor_session=',r.getheader('Set-Cookie',''))
            r.read();conn.close()
    def test_null_and_missing_origin_require_matching_csrf_cookie(self):
        token='a'*43
        for origin in ('null',''):
            headers={'Origin':origin,'Cookie':'__Host-monitor_csrf='+token}
            self.assertTrue(login_csrf_valid(headers,{'csrf':[token]}))
            self.assertFalse(login_csrf_valid(headers,{'csrf':['wrong']}))
            self.assertFalse(login_csrf_valid({'Origin':origin},{'csrf':[token]}))
        self.assertFalse(login_csrf_valid({'Origin':'https://evil.example','Cookie':'__Host-monitor_csrf='+token},{'csrf':[token]}))
    def test_render_external_origin_matches_even_with_internal_host(self):
        with patch.dict(os.environ,{'RENDER_EXTERNAL_URL':'https://monitor.onrender.com'}):
            self.assertTrue(same_origin({'Origin':'https://monitor.onrender.com','Host':'internal:10000'}))
            self.assertFalse(same_origin({'Origin':'https://evil.example','Host':'internal:10000'}))
    def test_null_origin_can_finish_real_form_login(self):
        conn,r=self.login('username=carlo&password='+self.password,'null')
        self.assertEqual(r.status,303);self.assertIn('monitor_session=',r.getheader('Set-Cookie',''))
        r.read();conn.close()
    def test_health_is_public_but_does_not_claim_operational(self):
        with self.request('/health') as r:
            self.assertEqual(r.status,200);self.assertIn(b'false',r.read())
    def test_password_must_be_strong_and_exact(self):
        self.assertTrue(authorized(self.auth,self.password))
        self.assertFalse(authorized(self.auth,'short'))
        self.assertFalse(authorized(self.auth+'x',self.password))
    def test_cross_origin_and_missing_custom_header_rejected(self):
        for extras in ({},{'Origin':'https://evil.example','X-Monitor-Action':'1'}):
            with self.request('/action',{'Authorization':self.auth,**extras},b'{"action":"start"}') as r:self.assertEqual(r.status,403)
        self.assertEqual(self.fake.calls,[])
    def test_authenticated_same_origin_start_allowed(self):
        host='127.0.0.1:'+str(self.server.server_port)
        headers={'Authorization':self.auth,'Origin':'https://'+host,'X-Monitor-Action':'1'}
        with self.request('/action',headers,b'{"action":"start","send_telegram":false}') as r:self.assertEqual(r.status,200)
        self.assertEqual(self.fake.calls,['start'])

if __name__=='__main__':unittest.main()

