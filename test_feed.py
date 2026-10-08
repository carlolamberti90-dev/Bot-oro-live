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

    def test_watchdog_reconnects_socket_with_no_new_prices(self):
        main.active_socket = Mock()
        main.ws_connected = True
        main.last_connection_time = 1000
        main.check_feed_connection(now=1181)
        main.active_socket.close.assert_called_once()

    def test_watchdog_rejects_fresh_transport_with_old_source_prices(self):
        main.active_socket = Mock()
        main.ws_connected = True
        main.last_connection_time = main.last_tick_time = 1200
        main.last_feed_timestamp = 900
        main.check_feed_connection(now=1201)
        main.active_socket.close.assert_called_once()
        with patch.object(main.time, 'time', return_value=1201):
            self.assertEqual(main.health_snapshot()['status'], 'feed_stale')

    def test_watchdog_keeps_healthy_connection_open(self):
        main.active_socket = Mock()
        main.ws_connected = True
        main.last_connection_time = main.last_tick_time = main.last_feed_timestamp = 1200
        main.history_retry_after = 1500
        main.check_feed_connection(now=1201)
        main.active_socket.close.assert_not_called()

    def test_history_retry_runs_even_on_healthy_connection(self):
        main.active_socket = Mock()
        main.ws_connected = True
        main.last_connection_time = main.last_tick_time = main.last_feed_timestamp = 1200
        main.history_retry_after = 1200
        with patch.object(main.requests, 'get', return_value=Mock(status_code=403)) as request:
            main.check_feed_connection(now=1201)
        request.assert_called_once()
        main.active_socket.close.assert_not_called()
        self.assertEqual(main.history_retry_after, 4801)

    def test_available_history_triggers_full_recovery(self):
        main.active_socket = Mock()
        main.ws_connected = True
        main.last_connection_time = main.last_tick_time = main.last_feed_timestamp = 1200
        main.history_retry_after = 1200
        response = Mock(status_code=200)
        with patch.object(main.requests, 'get', return_value=response), patch.object(
                main, 'parse_forex_candles', return_value=[Mock()] * 200):
            main.check_feed_connection(now=1201)
        main.active_socket.close.assert_called_once()
        self.assertEqual(main.history_retry_after, 0)

    def test_closed_socket_cannot_remain_subscription_verified(self):
        main.subscription_verified = True
        main.on_close(Mock(), 1000, 'closed')
        self.assertFalse(main.subscription_verified)

if __name__ == '__main__':
    unittest.main()
