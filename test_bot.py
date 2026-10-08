import importlib
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import main


class DetectorTests(unittest.TestCase):
    def setUp(self):
        importlib.reload(main)
        main.AUDIT_ENABLED = False
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        main.STATE_FILE = Path(self.temp.name) / 'state.json'

    def block(self, tf='M15', bullish=True, block_id='zone'):
        block = main.OrderBlock(block_id, 102, 99, 10, bullish, 100, 1)
        main.order_blocks[tf] = [block]
        return block

    def test_saved_oanda_history_replays_without_changing_live_state(self):
        self.block()
        candle = self.candle()
        main.close_candle('M15', candle)
        before = main.snapshot_structure('M15')
        live_history = main.history['M15']
        main.load_historical_reference()
        self.assertEqual(main.historical_reference['H4']['closed_candles'], 219)
        self.assertEqual(main.historical_reference['M15']['closed_candles'], 136)
        self.assertEqual(main.historical_reference['M3']['closed_candles'], 40)
        self.assertEqual(main.historical_reference['M5']['closed_candles'], 24)
        self.assertFalse(main.historical_reference['H4']['usable_for_live_alerts'])
        self.assertNotIn('D1', main.historical_reference)
        self.assertEqual(main.snapshot_structure('M15'), before)
        self.assertIs(main.history['M15'], live_history)
        self.assertTrue(main.bubble_queue.empty())
        self.assertTrue(main.telegram_queue.empty())

    def candle(self, tf='M15', period=1, low=98, high=103, close=100):
        candle = main.Candle(period, period * main.TIMEFRAMES[tf], 100, high, low, close, 10)
        main.current_candles[tf] = candle
        return candle

    def test_long_raid_is_green_and_only_sent_once(self):
        self.block()
        candle = self.candle()
        main.detect_manipulation_bubble('M15', candle)
        main.detect_manipulation_bubble('M15', candle)
        event = main.bubble_queue.get_nowait()
        self.assertTrue(main.bubble_queue.empty())
        self.assertEqual(event.direction, 'LONG')
        self.assertIn('🟢', main.format_event(event))
        self.assertTrue(event.event_id)

    def test_short_raid_is_red(self):
        self.block(bullish=False)
        main.detect_manipulation_bubble('M15', self.candle())
        event = main.bubble_queue.get_nowait()
        self.assertEqual(event.direction, 'SHORT')
        self.assertIn('🔴', main.format_event(event))

    def test_raid_requires_reentry(self):
        self.block()
        main.detect_manipulation_bubble('M15', self.candle(close=98))
        self.assertTrue(main.bubble_queue.empty())

    def test_new_candle_or_different_block_is_new_event(self):
        main.current_candles['M15'] = self.candle()
        self.assertTrue(main.alert_allowed('M15', 'LONG', 'a'))
        self.assertFalse(main.alert_allowed('M15', 'LONG', 'a'))
        self.assertTrue(main.alert_allowed('M15', 'LONG', 'b'))
        main.current_candles['M15'].period += 1
        self.assertTrue(main.alert_allowed('M15', 'LONG', 'a'))

    def test_duplicate_identity_survives_restart(self):
        self.candle()
        self.assertTrue(main.alert_allowed('M15', 'LONG', 'zone'))
        main.save_state()
        main.last_alert.clear()
        main.load_state()
        self.assertFalse(main.alert_allowed('M15', 'LONG', 'zone'))

    def test_m1_requires_both_live_confirmations(self):
        with patch.object(main.time, 'time', return_value=1000):
            for tf in ('M1', 'M3', 'M5'):
                self.block(tf)
                self.candle(tf, period=1000 // main.TIMEFRAMES[tf])
            main.detect_manipulation_bubble('M1', main.current_candles['M1'])
            self.assertTrue(main.bubble_queue.empty())
            for tf in ('M3', 'M5', 'M1'):
                main.detect_manipulation_bubble(tf, main.current_candles[tf])
            self.assertEqual([main.bubble_queue.get_nowait().tf for _ in range(3)], ['M3', 'M5', 'M1'])

    def test_queue_does_not_revive_invalidated_confirmation(self):
        event = main.BubbleEvent('M3', 'LONG', 100, 102, 99, 100, 1, 1, 10, 1000, 'zone')
        m1 = main.BubbleEvent('M1', 'LONG', 100, 102, 99, 100, 1, 1, 10, 1001, 'zone')
        main.confirmations['M5'] = main.BubbleEvent('M5', 'LONG', 100, 102, 99, 100, 1, 1, 10, 1000, 'zone')
        self.assertEqual(main.filter_m1_confirmation([event, m1], now=1002, refresh_confirmations=False), [event])
        self.assertNotIn('M3', main.confirmations)

    def test_old_tick_does_not_change_price_or_candle(self):
        main.process_tick(100, 1, 1000000)
        original = main.current_candles['M1'].volume
        self.assertFalse(main.process_tick(90, 1, 999000))
        self.assertEqual(main.last_price, 100)
        self.assertEqual(main.current_candles['M1'].volume, original)

    def test_invalid_tick_cannot_confirm_feed(self):
        main.last_tick_time = 1000
        main.on_message(Mock(), json.dumps({'type': 'trade', 'data': [{'s': main.SYMBOL, 'p': 'bad', 't': 1000}]}))
        self.assertFalse(main.subscription_verified)

    def test_startup_partial_candles_are_not_signals(self):
        with patch.object(main, 'detect_manipulation_bubble') as detect:
            main.process_tick(100, 0, 1000000)
            self.assertFalse(main.current_candles['M1'].complete)
            detect.assert_not_called()
            main.process_tick(101, 0, 1020000)
            self.assertTrue(main.current_candles['M1'].complete)
            self.assertEqual(len(main.history['M1']), 0)

    def test_provisional_structure_is_replayed_from_committed_state(self):
        self.block()
        main.committed_structure['M15'] = main.snapshot_structure('M15')
        main.invalidate_blocks('M15', self.candle(close=98))
        self.assertFalse(any(b.active for b in main.order_blocks['M15']))
        main.restore_committed_structure('M15')
        self.assertTrue(main.order_blocks['M15'][0].active)

    def test_h4_and_weekly_boundaries(self):
        def ts(value):
            return datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp()
        before, after = ts('2026-10-07T20:59:59'), ts('2026-10-07T21:00:00')
        self.assertNotEqual(main.candle_period('H4', before), main.candle_period('H4', after))
        self.assertEqual(main.candle_period('H4', after), main.candle_period('H4', after + 14399))
        sunday, monday = ts('2026-10-11T23:59:59'), ts('2026-10-12T00:00:00')
        self.assertNotEqual(main.candle_period('W1', sunday), main.candle_period('W1', monday))

    def test_online_feed_is_not_all_timeframes_ready(self):
        main.ws_connected = True
        main.last_tick_time = main.time.time()
        main.subscription_verified = True
        snap = main.health_snapshot()
        self.assertEqual(snap['status'], 'online')
        self.assertFalse(snap['all_timeframes_ready'])
        self.assertEqual(snap['ready_timeframes'], [])
        self.assertFalse(snap['history_recovery_complete'])
        self.assertEqual(snap['warmup_closed_candles_required'], 199)

    def test_full_volume_window_required_for_readiness(self):
        main.history['M1'].extend(main.Candle(i, i*60, 100, 102, 99, 101, 10) for i in range(22))
        self.assertFalse(main.health_snapshot()['ready']['M1'])
        main.history['M1'].extend(main.Candle(i, i*60, 100, 102, 99, 101, 10) for i in range(22,199))
        self.assertTrue(main.health_snapshot()['ready']['M1'])

    def test_multi_timeframe_pipeline_reaches_telegram_without_network(self):
        events = []
        for tf in ('M15', 'M30'):
            self.block(tf)
            main.detect_manipulation_bubble(tf, self.candle(tf))
            events.append(main.bubble_queue.queue[-1])
        main.MULTI_TF_WINDOW_SECONDS = 0.05
        original = main.enqueue_telegram
        def enqueue_and_stop(*args):
            original(*args)
            main.shutdown_event.set()
        with patch.object(main, 'enqueue_telegram', side_effect=enqueue_and_stop):
            main.bubble_aggregator()
        job = main.telegram_queue.queue[0]
        self.assertIn('MULTI-TIMEFRAME: M15 • M30', job['text'])
        self.assertEqual(job['event_ids'], [e.event_id for e in events])
        main.shutdown_event.clear()
        session = Mock()
        def sent(*args, **kwargs):
            main.shutdown_event.set()
            return Mock(ok=True, status_code=200)
        session.post.side_effect = sent
        with patch.object(main.requests, 'Session', return_value=session):
            main.telegram_worker()
        self.assertEqual(session.post.call_args.kwargs['json']['text'], job['text'])
        self.assertTrue(main.telegram_queue.empty())


if __name__ == '__main__':
    unittest.main()
