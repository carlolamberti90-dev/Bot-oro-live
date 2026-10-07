import unittest
from bubble_engine import BubbleEngine, Block, Candle, group_events

class EngineTests(unittest.TestCase):
    def bar(self, t, low, high, close):
        return Candle(t, close, high, low, close, 100)

    def test_intrabar_rollback_and_dedup(self):
        e = BubbleEngine()
        e.state.blocks = [Block(110, 100, 100, True)]
        self.assertEqual(e.update(self.bar(1, 99, 105, 101)), ["LONG"])
        self.assertEqual(e.update(self.bar(1, 98, 105, 99)), [])
        self.assertEqual(e.update(self.bar(1, 98, 105, 101), closed=True), [])
        self.assertEqual(len(e.state.blocks), 1)
        self.assertEqual(e.update(self.bar(2, 99, 105, 101)), ["LONG"])

    def test_mitigated_blocks_do_not_signal(self):
        e = BubbleEngine()
        e.state.blocks = [Block(110, 100, 100, False)]
        self.assertEqual(e.update(self.bar(1, 105, 112, 111), closed=True), [])
        self.assertEqual(e.state.blocks, [])

    def test_short_and_simultaneous_grouping(self):
        e = BubbleEngine()
        e.state.blocks = [Block(110, 100, 100, False)]
        self.assertEqual(e.update(self.bar(1, 105, 112, 109)), ["SHORT"])
        self.assertEqual(len(group_events([("M15", "LONG", 101), ("H1", "LONG", 101)])), 1)

    def test_confirmed_pivot_then_break(self):
        e = BubbleEngine(pivot_length=1)
        for t, high, close in [(1, 10, 8), (2, 12, 9), (3, 11, 10)]:
            e.update(self.bar(t, 5, high, close), closed=True, notify=False)
        self.assertEqual(e.state.last_high, (12, 1))
        e.update(self.bar(4, 8, 14, 13), closed=True)
        self.assertEqual(e.state.blocks, [Block(12, 5, 100, True)])

    def test_reject_skipped_open_bar(self):
        e = BubbleEngine()
        e.update(self.bar(1, 5, 10, 8))
        with self.assertRaises(ValueError):
            e.update(self.bar(2, 5, 10, 8))

if __name__ == "__main__":
    unittest.main()
