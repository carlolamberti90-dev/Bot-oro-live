import os
import unittest
from unittest.mock import patch
import test_server
from server import Handler

class ConnectionPageTests(unittest.TestCase):
    def setUp(self):
        self.fixture = test_server.SecurityTests()
        self.fixture.setUp()
    def tearDown(self):
        self.fixture.tearDown()
    def test_anonymous_page_never_discloses_secret(self):
        with patch.dict(os.environ, {'TV_RELAY_SECRET': 'test-private-key-' * 3}):
            with self.fixture.request('/connect') as response:
                body = response.read()
                self.assertEqual(response.status, 200)
                self.assertIn(b'Entra nel monitor', body)
                self.assertNotIn(b'test-private-key-', body)
    def test_authenticated_owner_can_copy_configured_url(self):
        secret = 'test-private-key-' * 3
        with patch.dict(os.environ, {'TV_RELAY_SECRET': secret}), patch.object(Handler, 'relay', object()):
            with self.fixture.request('/connect', {'Authorization': self.fixture.auth}) as response:
                self.assertEqual(response.status, 200)
                self.assertIn(('/webhooks/tradingview/' + secret).encode(), response.read())
                self.assertEqual(response.headers['Cache-Control'], 'no-store')
                self.assertEqual(response.headers['Referrer-Policy'], 'no-referrer')
    def test_missing_configuration_does_not_show_a_broken_url(self):
        with patch.object(Handler, 'relay', None):
            with self.fixture.request('/connect', {'Authorization': self.fixture.auth}) as response:
                self.assertEqual(response.status, 503)
                self.assertNotIn(b'/webhooks/tradingview/', response.read())
