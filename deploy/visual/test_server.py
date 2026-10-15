import base64
import os
import threading
import unittest
import urllib.request
import urllib.error
from http.server import ThreadingHTTPServer
from unittest.mock import patch
from server import Handler, authorized

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
        for path in ('/','/frame','/status','/setup.js'):
            with self.request(path) as r:self.assertEqual(r.status,401)
        self.assertEqual(self.fake.calls,[])
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
