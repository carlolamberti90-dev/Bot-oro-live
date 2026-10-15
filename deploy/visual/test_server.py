import base64
import http.client
import os
import threading
import unittest
import urllib.request
import urllib.error
from http.server import ThreadingHTTPServer
from unittest.mock import patch
from server import Handler, authorized, valid_session

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
        conn.request('POST','/login',body,{'Origin':origin or 'https://'+host,'Content-Type':'application/x-www-form-urlencoded'})
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
            self.assertEqual(r.status,code);self.assertIsNone(r.getheader('Set-Cookie'))
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
