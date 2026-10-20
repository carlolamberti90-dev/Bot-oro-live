import unittest
from tradingview_relay import parse_message, receive_http

class FakeHandler:
    path = '/webhooks/tradingview/test-secret'
    def reply(self, code, data):
        self.result = code, data

class RelayTests(unittest.TestCase):
    def test_actual_tradingview_message(self):
        message, timeframe = parse_message('SHORT 🔴 | OANDA:XAUUSD | TF 15 | Bubble appena comparsa | Prezzo 4126.870'.encode())
        self.assertEqual(timeframe, '15')
        self.assertIn('SHORT 🔴', message)

    def test_all_requested_timeframes(self):
        for tf in ('3','5','15','30','45','60','240','D','1D'):
            self.assertEqual(parse_message(f'LONG 🟢 | OANDA:XAUUSD | TF {tf} | Bubble appena comparsa | Prezzo 4132.5'.encode())[1], tf)

    def test_other_symbols_m1_and_arbitrary_text_rejected(self):
        for message in ('LONG 🟢 | OANDA:XAUUSD | TF 1 | Bubble appena comparsa | Prezzo 4132', 'LONG 🟢 | FX:XAUUSD | TF 3 | Bubble appena comparsa | Prezzo 4132', 'send this arbitrary message'):
            with self.assertRaises(ValueError):
                parse_message(message.encode())

    def test_zero_price_rejected(self):
        with self.assertRaises(ValueError):
            parse_message('LONG 🟢 | OANDA:XAUUSD | TF 3 | Bubble appena comparsa | Prezzo 0'.encode())

    def test_wrong_secret_never_accepts(self):
        class FakeRelay:
            secret = 'x' * 40
            def accept(self, raw):
                raise AssertionError('unauthorized request reached outbox')
        h = FakeHandler()
        self.assertTrue(receive_http(h, FakeRelay()))
        self.assertEqual(h.result[0], 401)

    def test_no_relay_returns_503(self):
        h = FakeHandler()
        receive_http(h, None)
        self.assertEqual(h.result[0], 503)

    def test_other_routes_unaffected(self):
        h = FakeHandler(); h.path='/login'
        self.assertFalse(receive_http(h, None))

if __name__ == '__main__':
    unittest.main()
