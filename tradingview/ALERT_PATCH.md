# Direct Pine bubble alerts

Use the user-supplied LuxAlgo Pine source as the base.

Add these two globals before the block-update loop:

```pine
bool alertRaidHigh = false
bool alertRaidLow = false
```

Inside the existing Manipulation Bubbles section, immediately after the original lines:

```pine
raidHigh = not ob.isBullish and high > ob.high and close <= ob.high
raidLow  = ob.isBullish and low < ob.low and close >= ob.low
```

add:

```pine
alertRaidHigh := alertRaidHigh or raidHigh
alertRaidLow  := alertRaidLow or raidLow
```

At the very end of the script add:

```pine
alertcondition(alertRaidHigh, title = "Manipulation Bubble SHORT", message = "{\"type\":\"manipulation_bubble\",\"direction\":\"SHORT\",\"ticker\":\"{{ticker}}\",\"interval\":\"{{interval}}\",\"close\":\"{{close}}\",\"time\":\"{{time}}\"}")
alertcondition(alertRaidLow, title = "Manipulation Bubble LONG", message = "{\"type\":\"manipulation_bubble\",\"direction\":\"LONG\",\"ticker\":\"{{ticker}}\",\"interval\":\"{{interval}}\",\"close\":\"{{close}}\",\"time\":\"{{time}}\"}")
```

This leaves the original detection logic unchanged and only exposes the already-existing bubble events as TradingView alert conditions.
