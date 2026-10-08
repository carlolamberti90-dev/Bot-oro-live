import gzip
import importlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
import main
from history_store import HistoryStore, merge_checkpoints

class HistoryTests(unittest.TestCase):
    def test_replacement_instance_preserves_older_candles_and_alert_ids(self):
        old = {'history': {'M1': [{'period': 1}, {'period': 2}]}, 'last_alert': {'old': 1}}
        new = {'history': {'M1': [{'period': 3}]}, 'history_limits': {'M1': 3}, 'last_alert': {'new': 2}}
        result = merge_checkpoints(old, new)
        self.assertEqual([c['period'] for c in result['history']['M1']], [1, 2, 3])
        self.assertEqual(result['replay_timeframes'], ['M1'])
        self.assertEqual(result['last_alert'], {'old': 1, 'new': 2})

    def test_retention_merge_prefers_new_candle_and_drops_only_oldest(self):
        old = {'history': {'M1': [{'period': 1}, {'period': 2, 'close': 100}]}}
        new = {'history': {'M1': [{'period': 2, 'close': 101}, {'period': 3}]}, 'history_limits': {'M1': 2}}
        result = merge_checkpoints(old, new)
        self.assertEqual(result['history']['M1'], new['history']['M1'])
        self.assertEqual(result['replay_timeframes'], [])

    def setUp(self):
        importlib.reload(main)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        main.STATE_FILE = Path(self.temp.name) / 'state.json'
        main.history_store = Mock(url='test')
        main.history_store.load.return_value = None

    def test_two_days_of_m1_survive_checkpoint_and_restore_without_alerts(self):
        for period in range(2880):
            main.history['M1'].append(main.Candle(period, period * 60, 100, 102, 99, 101, 10))
        main.order_blocks['M1'] = [main.OrderBlock('saved', 102, 99, 10, True, 100, 2870)]
        main.save_state()
        payload = main.history_store.save.call_args.args[0]
        main.STATE_FILE.unlink()
        main.history['M1'].clear()
        main.order_blocks['M1'] = []
        main.history_store.load.return_value = payload
        main.load_state()
        self.assertEqual(len(main.history['M1']), 2880)
        self.assertEqual(main.order_blocks['M1'][0].block_id, 'saved')
        self.assertTrue(main.bubble_queue.empty())

    def test_newer_local_checkpoint_wins_over_older_remote(self):
        main.save_state()
        remote = main.history_store.save.call_args.args[0].copy()
        remote['saved_at'] = 0
        remote['history'] = {}
        main.history_store.load.return_value = remote
        main.history['M1'].append(main.Candle(1, 60, 100, 102, 99, 101))
        main.save_state()
        main.history['M1'].clear()
        main.load_state()
        self.assertEqual(len(main.history['M1']), 1)

    def test_database_outage_does_not_erase_existing_checkpoint(self):
        store = HistoryStore('OANDA:XAU_USD')
        store.url = 'configured'
        with patch.object(store, 'connection', side_effect=RuntimeError('secret')):
            store.save({'saved_at': 1, 'history': {}})
            self.assertEqual(store.status, 'save_failed')
            self.assertIsNone(store.load())
            self.assertEqual(store.status, 'load_failed')

    def test_database_load_decodes_compressed_checkpoint(self):
        store = HistoryStore('OANDA:XAU_USD')
        store.url = 'configured'
        conn = Mock()
        conn.__enter__ = Mock(return_value=conn)
        conn.__exit__ = Mock(return_value=False)
        payload = {'saved_at': 12, 'history': {'M1': []}}
        conn.execute.return_value.fetchone.return_value = (gzip.compress(json.dumps(payload).encode()),)
        with patch.object(store, 'connection', return_value=conn):
            self.assertEqual(store.load(), payload)
        self.assertEqual(store.status, 'restored')
