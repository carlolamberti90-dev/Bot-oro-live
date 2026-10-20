import unittest
from webhook_receiver import validate


class ValidationTests(unittest.TestCase):
    def setUp(self):
        self.secret = 'x' * 40
        self.event = dict(secret=self.secret, symbol='OANDA:XAUUSD', timeframe='3',
                          direction='LONG', price=4132, candle_time=1000000,
                          observed_at=1001000, event_id='zone:candle:long')

    def test_valid_and_secret_removed(self):
        self.assertNotIn('secret', validate(self.event, self.secret, now=1001))

    def test_m1_rejected(self):
        self.event['timeframe'] = '1'
        with self.assertRaises(ValueError):
            validate(self.event, self.secret, now=1001)

    def test_wrong_secret(self):
        with self.assertRaises(PermissionError):
            validate(self.event, 'y' * 40, now=1001)

    def test_expired(self):
        with self.assertRaises(ValueError):
            validate(self.event, self.secret, now=2000)

    def test_non_finite(self):
        self.event['price'] = float('nan')
        with self.assertRaises(ValueError):
            validate(self.event, self.secret, now=1001)


if __name__ == '__main__':
    unittest.main()
