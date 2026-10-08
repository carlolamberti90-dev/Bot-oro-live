import unittest
from unittest.mock import patch, Mock
import main
import importlib

class FeedTests(unittest.TestCase):
    def setUp(self):
        importlib.reload(main)
        main.SYMBOL = 'OANDA:XAU_USD'
        main.feed_error = None
        main.last_tick_time = None
        main.subscription_verified = False

    def test_catalog_resolves_actual_symbol_without_changing_broker(self):
        r = Mock(status_code=200)
        r.json.return_value = [{'symbol': 'OANDA:XAUUSD'}]
        with patch.object(main.requests, 'get', return_value=r):
            self.assertTrue(main.resolve_symbol())
        self.assertEqual(main.SYMBOL, 'OANDA:XAUUSD')

    def test_missing_gold_does_not_switch_to_other_asset(self):
        r = Mock(status_code=200)
        r.json.return_value = [{'symbol': 'OANDA:EUR_USD'}, {'symbol': 'OTHER:XAU_USD'}]
        with patch.object(main.requests, 'get', return_value=r):
            self.assertFalse(main.resolve_symbol())
        self.assertEqual(main.health_snapshot()['status'], 'feed_error')

    def test_entitlement_error_is_visible(self):
        with patch.object(main.requests, 'get', return_value=Mock(status_code=403)):
            self.assertFalse(main.resolve_symbol())
        self.assertEqual(main.feed_error, 'symbol_catalog_http_403')

    def test_socket_connection_is_not_feed_confirmation(self):
        ws = Mock()
        with patch.object(main, "recover_history"):
            main.on_open(ws)
        self.assertFalse(main.health_snapshot()['subscription_verified'])
        self.assertEqual(main.health_snapshot()['status'], 'starting')

    def test_rejected_subscription_closes_socket(self):
        ws = Mock()
        main.on_message(ws, '{"type":"error","msg":"Invalid symbol"}')
        ws.close.assert_called_once()
        self.assertEqual(main.health_snapshot()['status'], 'feed_error')

if __name__ == '__main__':
    unittest.main()
