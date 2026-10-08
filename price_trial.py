"""Experimental price-only re-entry watches; no historical alerts or volumes."""
import hashlib


class PriceRaidTrial:
    def __init__(self, references, period_fn, restored=None):
        self.period_fn = period_fn
        self.zones = {}
        self.last_timestamp = None
        self.last_price = None
        self.sequence = 0
        for tf, reference in references.items():
            for block in reference['zones']:
                key = block['block_id']
                self.zones[key] = dict(tf=tf, low=block['low'], high=block['high'],
                                       bullish=block['bullish'], active=True,
                                       armed=False, period=None)
        if restored:
            for key in restored.get('inactive', []):
                if key in self.zones:
                    self.zones[key]['active'] = False

    def state(self):
        # Never restore an excursion across an unobserved restart interval.
        return {'inactive': [key for key, z in self.zones.items() if not z['active']]}

    def process(self, price, timestamp):
        if self.last_timestamp is not None and timestamp <= self.last_timestamp:
            return []
        gap = self.last_timestamp is None or timestamp - self.last_timestamp > 180
        events = []
        for key, zone in self.zones.items():
            if not zone['active']:
                continue
            period = self.period_fn(zone['tf'], timestamp)
            if zone['period'] is not None and period != zone['period']:
                # Last observed close outside the zone invalidates it, as in Pine.
                outside = self.last_price < zone['low'] if zone['bullish'] else self.last_price > zone['high']
                if outside and not gap:
                    zone['active'] = False
                    continue
                zone['armed'] = False
            zone['period'] = period
            if gap:
                zone['armed'] = False
                continue
            level = zone['low'] if zone['bullish'] else zone['high']
            outside = price < level if zone['bullish'] else price > level
            if outside:
                zone['armed'] = True
            elif zone['armed']:
                zone['armed'] = False
                self.sequence += 1
                direction = 'LONG' if zone['bullish'] else 'SHORT'
                event_id = hashlib.sha256(f'trial:{key}:{timestamp}:{self.sequence}'.encode()).hexdigest()[:20]
                events.append(dict(event_id=event_id, tf=zone['tf'], direction=direction,
                                   price=price, zone_low=zone['low'], zone_high=zone['high'],
                                   feed_timestamp=timestamp, block_id=key))
        self.last_timestamp = timestamp
        self.last_price = price
        return events


def format_trial(events):
    lines = ['🧪 PROVA — superamento e rientro nella zona',
             'Zone OANDA del 7 ottobre; validità attuale da verificare.']
    for event in events:
        icon = '🟢' if event['direction'] == 'LONG' else '🔴'
        lines.append(f"{icon} {event['tf']} {event['direction']} | Prezzo {event['price']:.3f} | Zona {event['zone_low']:.3f}–{event['zone_high']:.3f}")
    return '\n'.join(lines)
