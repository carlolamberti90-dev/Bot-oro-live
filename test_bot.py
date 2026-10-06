import unittest
from unittest.mock import patch
import main

class WebhookTests(unittest.TestCase):
    def setUp(self):
        main.last_event.clear()
        main.last_signal_time = None

    def test_normalize_long(self):
        e = main.normalize_payload({
            "type": "manipulation_bubble",
            "direction": "LONG",
            "ticker": "OANDA:XAUUSD",
            "interval": "5",
            "close": "2650.1",
            "time": "2026-10-06T15:00:00Z",
        })
        self.assertEqual(e["direction"], "LONG")
        self.assertEqual(e["interval"], "5")

    def test_reject_invalid_direction(self):
        with self.assertRaises(ValueError):
            main.normalize_payload({"direction": "BUY"})

    def test_duplicate_detection(self):
        e = {
            "direction": "SHORT",
            "ticker": "OANDA:XAUUSD",
            "interval": "15",
            "time": "t",
            "close": "1",
        }
        self.assertFalse(main.is_duplicate(e))
        self.assertTrue(main.is_duplicate(e))

    def test_message_format(self):
        e = {
            "direction": "LONG",
            "ticker": "OANDA:XAUUSD",
            "interval": "1",
            "time": "",
            "close": "2650.5",
        }
        msg = main.format_message(e)
        self.assertIn("LONG", msg)
        self.assertIn("2650.5", msg)

    def test_health(self):
        snap = main.health_snapshot()
        self.assertEqual(snap["status"], "online")

if __name__ == "__main__":
    unittest.main()
