# Derived from Volumetric Order Flow Structure, copyright LuxAlgo.
# Adaptation: Python signal engine; drawing code omitted.
# Licensed under CC BY-NC-SA 4.0:
# https://creativecommons.org/licenses/by-nc-sa/4.0/
"""Bubble conditions ported from Oro_Bubble_Alerts.pine.

Consumes chronological OHLCV snapshots for ONE timeframe. The caller must
provide history from the same feed and session alignment as the chart.
Intrabar state is rebuilt from the previous closed bar (Pine rollback).
Sensitivity changes bubble size, not the signal condition.
"""
from dataclasses import dataclass
from math import isfinite


@dataclass(frozen=True)
class Candle:
    time: int
    open: float
    high: float
    low: float
    close: float
    volume: float

    def validate(self):
        if not all(isfinite(v) for v in (self.open, self.high, self.low, self.close, self.volume)):
            raise ValueError("Non-finite OHLCV")
        if self.low > min(self.open, self.close) or self.high < max(self.open, self.close) or self.low > self.high or self.volume < 0:
            raise ValueError("Invalid OHLCV")


@dataclass(frozen=True)
class Block:
    high: float
    low: float
    volume: float
    bullish: bool


@dataclass
class State:
    blocks: list
    last_high: tuple | None = None
    last_low: tuple | None = None


class BubbleEngine:
    def __init__(self, pivot_length=3, max_blocks=4, hide_overlapping=True):
        if pivot_length < 1 or max_blocks < 1:
            raise ValueError("Parameters must be positive")
        self.pivot_length = pivot_length
        self.max_blocks = max_blocks
        self.hide_overlapping = hide_overlapping
        self.history = []
        self.state = State([])
        self.current_time = None
        self.alerted = set()

    def _calculate(self, candle):
        bars = self.history + [candle]
        n = len(bars) - 1
        state = State(list(self.state.blocks), self.state.last_high, self.state.last_low)
        p = self.pivot_length
        if n >= 2 * p:
            index = n - p
            center = bars[index]
            left, right = bars[index-p:index], bars[index+1:index+p+1]
            # Latest equal extreme wins: left equality allowed, right not.
            if all(center.high >= b.high for b in left) and all(center.high > b.high for b in right):
                state.last_high = (center.high, index)
            if all(center.low <= b.low for b in left) and all(center.low < b.low for b in right):
                state.last_low = (center.low, index)

        signals = set()
        retained = []
        for block in state.blocks:
            if (block.bullish and candle.close < block.low) or (not block.bullish and candle.close > block.high):
                continue
            retained.append(block)
            if block.bullish and candle.low < block.low and candle.close >= block.low:
                signals.add("LONG")
            if not block.bullish and candle.high > block.high and candle.close <= block.high:
                signals.add("SHORT")
        state.blocks = retained

        previous = self.history[-1] if self.history else None
        # ta.crossover uses the previous bar's pivot SERIES value.
        cross_up = previous is not None and state.last_high is not None and self.state.last_high is not None and candle.close > state.last_high[0] and previous.close <= self.state.last_high[0]
        cross_down = previous is not None and state.last_low is not None and self.state.last_low is not None and candle.close < state.last_low[0] and previous.close >= self.state.last_low[0]
        for crossing, pivot, bullish in ((cross_up, state.last_high, True), (cross_down, state.last_low, False)):
            if not crossing:
                continue
            source = bars[pivot[1]]
            overlaps = [b for b in state.blocks if source.low < b.high and source.high > b.low] if self.hide_overlapping else []
            if not any(source.volume <= b.volume for b in overlaps):
                state.blocks = [b for b in state.blocks if b not in overlaps]
                state.blocks.append(Block(source.high, source.low, source.volume, bullish))
                state.blocks = state.blocks[-self.max_blocks:]
            if bullish:
                state.last_high = None
            else:
                state.last_low = None
        return state, signals

    def update(self, candle, *, closed=False, notify=True):
        """Feed every closed bar, then updates of the current bar.

        Seed history with notify=False. Each direction emits once per candle.
        Missing bars must be backfilled before resuming after disconnection.
        """
        candle.validate()
        if self.history and candle.time <= self.history[-1].time:
            raise ValueError("Closed candle cannot be revised")
        if self.current_time is not None and candle.time != self.current_time:
            raise ValueError("Close current candle before advancing")
        if self.current_time is None:
            self.current_time = candle.time
            self.alerted = set()
        state, signals = self._calculate(candle)
        events = sorted(signals - self.alerted) if notify else []
        self.alerted.update(signals)
        if closed:
            self.history.append(candle)
            self.state = state
            self.current_time = None
        return events


def group_events(events):
    """Group one polling batch; later batches remain separate events."""
    grouped = {}
    for timeframe, direction, price in events:
        grouped.setdefault(direction, {})[timeframe] = price
    return [
        ("🟢 LONG" if direction == "LONG" else "🔴 SHORT")
        + " | " + ", ".join(f"{tf}: {price}" for tf, price in frames.items())
        for direction, frames in grouped.items()
    ]
