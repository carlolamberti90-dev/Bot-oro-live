import unittest
from price_trial import PriceRaidTrial, format_trial

class TrialTests(unittest.TestCase):
    def watcher(self, bullish=False, restored=None):
        refs={'M15':{'zones':[dict(block_id='z',low=99,high=102,bullish=bullish)]}}
        return PriceRaidTrial(refs,lambda tf,t:int(t//900),restored)

    def test_short_requires_observed_excursion_and_reentry(self):
        w=self.watcher()
        self.assertEqual(w.process(101,1),[])
        self.assertEqual(w.process(103,2),[])
        events=w.process(102,3)
        self.assertEqual(events[0]['direction'],'SHORT')
        self.assertEqual(w.process(101,4),[])
        self.assertIn('PROVA',format_trial(events))
        self.assertNotIn('Volume',format_trial(events))

    def test_long_reentry(self):
        w=self.watcher(True);w.process(100,1);w.process(98,2)
        self.assertEqual(w.process(99,3)[0]['direction'],'LONG')

    def test_new_excursion_is_new_event(self):
        w=self.watcher();w.process(101,1);w.process(103,2)
        first=w.process(101,3)[0];w.process(103,4)
        self.assertNotEqual(first['event_id'],w.process(101,5)[0]['event_id'])

    def test_startup_never_sends_historical_raid(self):
        w=self.watcher();self.assertEqual(w.process(103,1),[])
        self.assertEqual(w.process(101,2),[])

    def test_gap_does_not_create_reentry(self):
        w=self.watcher();w.process(101,1);w.process(103,2)
        self.assertEqual(w.process(101,190),[])

    def test_out_of_order_or_duplicate_tick_ignored(self):
        w=self.watcher();w.process(101,1);w.process(103,3)
        self.assertEqual(w.process(101,2),[])
        self.assertEqual(w.process(101,3),[])
        self.assertEqual(w.process(101,4)[0]['direction'],'SHORT')

    def test_close_beyond_zone_invalidates_and_survives_reload(self):
        w=self.watcher();w.process(101,898);w.process(103,899)
        self.assertEqual(w.process(101,900),[])
        restored=self.watcher(restored=w.state())
        self.assertFalse(restored.zones['z']['active'])

    def test_restart_does_not_restore_armed_excursion(self):
        w=self.watcher();w.process(101,1);w.process(103,2)
        restored=self.watcher(restored=w.state())
        self.assertEqual(restored.process(101,3),[])

if __name__=='__main__': unittest.main()
