import unittest
from unittest.mock import patch
import main

class DetectorTests(unittest.TestCase):
    def setUp(self):
        for tf in main.TIMEFRAMES:
            main.history[tf].clear()
            main.order_blocks[tf].clear()
            main.current_candles[tf] = None
            main.last_pivot_high[tf] = None
            main.last_pivot_low[tf] = None
            main.last_pivot_high_candle[tf] = None
            main.last_pivot_low_candle[tf] = None
            main.trend_state[tf] = 0
        main.last_alert.clear()

    def candle(self, period, o=100, h=101, l=99, c=100, v=10):
        x = main.Candle(period, period * 60, o, h, l, c, v, 10)
        main.profile_add(x, c, v)
        return x

    def test_profile_poc_is_inside_candle(self):
        c = self.candle(1)
        poc = main.calculate_poc(c)
        self.assertGreaterEqual(poc, c.low)
        self.assertLessEqual(poc, c.high)

    def test_long_raid_enqueues_bubble(self):
        tf = "M1"
        for i in range(main.VOLUME_LOOKBACK):
            main.history[tf].append(self.candle(i))
        main.order_blocks[tf].append(main.OrderBlock("b", 100, 99, 10, True, 99.5, 1))
        raid = self.candle(100, o=100, h=101, l=98.5, c=99.5, v=20)
        with patch.object(main, "enqueue_bubble") as enqueue:
            main.detect_manipulation_bubble(tf, raid)
            self.assertTrue(enqueue.called)
            self.assertEqual(enqueue.call_args.args[0].direction, "LONG")

    def test_short_raid_enqueues_bubble(self):
        tf = "M1"
        for i in range(main.VOLUME_LOOKBACK):
            main.history[tf].append(self.candle(i))
        main.order_blocks[tf].append(main.OrderBlock("s", 101, 100, 10, False, 100.5, 1))
        raid = self.candle(100, o=100, h=101.5, l=99, c=100.5, v=20)
        with patch.object(main, "enqueue_bubble") as enqueue:
            main.detect_manipulation_bubble(tf, raid)
            self.assertTrue(enqueue.called)
            self.assertEqual(enqueue.call_args.args[0].direction, "SHORT")

    def test_cooldown_blocks_duplicate(self):
        self.assertTrue(main.alert_allowed("M1", "LONG", "x"))
        self.assertFalse(main.alert_allowed("M1", "LONG", "x"))

    def test_structure_break_creates_bullish_block(self):
        tf = "M1"
        pivot = self.candle(10, o=99, h=100, l=98, c=99, v=12)
        main.last_pivot_high[tf] = 100
        main.last_pivot_high_candle[tf] = pivot
        main.history[tf].append(self.candle(20, o=99, h=100, l=98, c=99.5, v=10))
        breakout = self.candle(21, o=99.5, h=101, l=99, c=100.5, v=15)
        main.detect_structure_break(tf, breakout)
        active = [b for b in main.order_blocks[tf] if b.active]
        self.assertEqual(len(active), 1)
        self.assertTrue(active[0].bullish)
        self.assertEqual(active[0].high, pivot.high)
        self.assertEqual(active[0].low, pivot.low)

    def test_bubble_scale_matches_max_200_volume(self):
        tf = "M1"
        for i in range(10):
            main.history[tf].append(self.candle(i, v=10))
        main.order_blocks[tf].append(main.OrderBlock("b", 100, 99, 10, True, 99.5, 1))
        raid = self.candle(100, o=100, h=101, l=98.5, c=99.5, v=20)
        with patch.object(main, "enqueue_bubble") as enqueue:
            main.detect_manipulation_bubble(tf, raid)
            event = enqueue.call_args.args[0]
            self.assertAlmostEqual(event.bubble_scale, main.BUBBLE_SENSITIVITY)

    def test_prune_bounds_inactive_blocks(self):
        tf = "M1"
        main.order_blocks[tf] = [
            main.OrderBlock(str(i), 101, 100, 1, True, 100.5, i, active=False)
            for i in range(100)
        ]
        main.prune_blocks(tf)
        self.assertLessEqual(len(main.order_blocks[tf]), 20)

if __name__ == "__main__":
    unittest.main()
