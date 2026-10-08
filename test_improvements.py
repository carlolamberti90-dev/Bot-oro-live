import unittest
from unittest.mock import Mock, patch
import main
import importlib

class Improvements(unittest.TestCase):
    def setUp(self):
        importlib.reload(main)
        main.confirmations.clear()
    def event(self, tf, stamp, direction='LONG'):
        return main.BubbleEvent(tf,direction,100,101,99,100,1,1,10,stamp,'test')
    def test_confirmation_survives_three_seconds(self):
        main.filter_m1_confirmation([self.event('M3',900),self.event('M5',900)],now=904)
        event=self.event('M1',960)
        self.assertEqual(main.filter_m1_confirmation([event],now=964),[event])
    def test_expired_confirmation_does_not_block_m1(self):
        main.filter_m1_confirmation([self.event('M3',900),self.event('M5',900)],now=904)
        event = self.event('M1',1080)
        self.assertEqual(main.filter_m1_confirmation([event],now=1084),[event])
    def test_opposite_confirmation_does_not_block_m1(self):
        self.assertEqual(main.filter_m1_confirmation([self.event('M1',900), self.event('M3',900),self.event('M5',900,'SHORT')],now=904)[0].tf,'M1')
    def test_same_close_order_independent(self):
        events=[self.event('M1',900),self.event('M5',900),self.event('M3',900)]
        self.assertEqual(main.filter_m1_confirmation(events,now=904),events)
    def test_event_delivery_independent_of_future_confirmation(self):
        events=[self.event('M1',840),self.event('M3',900),self.event('M5',900)]
        self.assertIn(events[0],main.filter_m1_confirmation(events,now=904))
    def payload(self):
        return dict(s='ok',t=[0,60,120],o=[100]*3,h=[102]*3,l=[99]*3,c=[101]*3,v=[10]*3)
    def test_parse_excludes_open_candle(self):
        self.assertEqual(len(main.parse_forex_candles(self.payload(),60,130)),2)
    def test_parse_rejects_malformed_data(self):
        for field,value in [('v',[10]),('h',[98]*3),('c',[float('nan')]*3),('t',[60,0,120])]:
            data=self.payload();data[field]=value
            with self.assertRaises(ValueError):main.parse_forex_candles(data,60,180)
    def test_aggregation_requires_complete_contiguous_group(self):
        candles=main.parse_forex_candles(self.payload(),60,180)
        result=main.aggregate_history(candles,60,180)
        self.assertEqual((len(result),result[0].volume),(1,30))
        self.assertEqual(main.aggregate_history(candles[1:],60,180),[])
    def test_denied_history_not_fabricated(self):
        main.history_retry_after=0
        with patch.object(main.requests,'get',return_value=Mock(status_code=403)):
            main.recover_history()
        self.assertTrue(all(v=='history_http_403' for v in main.history_status.values()))
    def test_touching_zones_do_not_overlap(self):
        self.assertFalse(main.zones_overlap(Mock(low=1,high=2),Mock(low=2,high=3)))
    def test_latest_equal_pivot_wins(self):
        candles=[Mock(high=x) for x in [1,2,3,3,2,1,1]]
        self.assertTrue(main.is_pivot_high(candles,3))
        candles[4].high=3
        self.assertFalse(main.is_pivot_high(candles,3))
    def test_volume_window_includes_current_in_200_bars(self):
        saved=list(main.history["M1"])
        main.history["M1"].clear()
        main.history["M1"].extend(main.Candle(i,i*60,100,102,99,101,1000 if i==0 else 10) for i in range(200))
        try:self.assertEqual(main.max_volume_lookback("M1",20),20)
        finally:
            main.history["M1"].clear();main.history["M1"].extend(saved)
    def test_history_replay_silent(self):
        candle=main.Candle(0,0,100,102,99,101,10)
        with patch.object(main,'detect_manipulation_bubble') as detect:
            main.close_candle('M1',candle,notify=False)
        detect.assert_not_called()

if __name__=='__main__':unittest.main()
