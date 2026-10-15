"""Association through the visible Data window; server integration needs live QA.

LuxAlgo places all same-direction bubble labels on a candle at the same high
or low. Coincident labels are therefore one observable candle/direction event,
not separately identifiable labels. Pixel coordinates and volume are not IDs.
"""
import re
from datetime import datetime
from zoneinfo import ZoneInfo

INTERVAL_TEXT = {'M1':'1','M3':'3','M5':'5','M15':'15','M30':'30','M45':'45',
                 'H1':'1h','H4':'4h','D1':'1D'}
MONTHS = dict(zip(('Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'),range(1,13)))


def parse_data_window(text, tf, timezone_name):
    """Parse only an explicit date, time and matching instrument/interval."""
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    header = f'XAUUSD · {INTERVAL_TEXT[tf]} · OANDA'
    if header not in lines:
        raise ValueError('data_window_symbol_or_interval_mismatch')
    try:
        date = lines[lines.index('Date')+1]
        clock = lines[lines.index('Time')+1] if 'Time' in lines else None
    except (ValueError, IndexError):
        raise ValueError('data_window_datetime_missing') from None
    match = re.fullmatch(r'(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun) (\d{2}) ([A-Z][a-z]{2}) [\u2019\'](\d{2})',date)
    if not match or match[2] not in MONTHS:
        raise ValueError('data_window_datetime_unrecognized')
    if tf=='D1' and clock is None:
        # Daily UI supplies a trading date, not the original opening timestamp.
        return datetime(2000+int(match[3]),MONTHS[match[2]],int(match[1])).date().isoformat()
    if not clock or not re.fullmatch(r'\d{2}:\d{2}',clock):
        raise ValueError('data_window_time_missing')
    hour, minute = map(int,clock.split(':'))
    zone=ZoneInfo(timezone_name)
    naive=datetime(2000+int(match[3]),MONTHS[match[2]],int(match[1]),hour,minute)
    local=naive.replace(tzinfo=zone)
    # Reject DST ambiguity and non-existent local dates rather than choose a bar.
    if local.utcoffset()!=naive.replace(tzinfo=zone,fold=1).utcoffset():
        raise ValueError('ambiguous_local_candle_time')
    from datetime import timezone
    if local.astimezone(timezone.utc).astimezone(zone).replace(tzinfo=None)!=naive:
        raise ValueError('nonexistent_local_candle_time')
    return local.astimezone(timezone.utc).isoformat()


class DataWindowAssociator:
    def __init__(self, page, timezone_name):
        self.page=page
        self.timezone_name=timezone_name
        ZoneInfo(timezone_name)

    def prepare(self):
        button=self.page.get_by_role('button',name='Object tree and data window',exact=True)
        if button.get_attribute('aria-pressed')!='true':
            button.click()
        self.page.get_by_role('tab',name='Data window',exact=True).click()
        self.page.get_by_role('treegrid').wait_for(state='visible',timeout=10000)
        caption=self.page.get_by_role('button',name='Timezone',exact=True).inner_text()
        offset=datetime.now(ZoneInfo(self.timezone_name)).utcoffset().total_seconds()/3600
        displayed=re.search(r'UTC([+-]\d+(?::\d{2})?)',caption)
        if not displayed:
            if not (offset==0 and 'UTC' in caption):raise ValueError('chart_timezone_unverified')
        else:
            parts=displayed[1].split(':');actual=int(parts[0])
            if len(parts)>1:actual+=(1 if displayed[1][0]=='+' else -1)*int(parts[1])/60
            if actual!=offset:raise ValueError('chart_timezone_mismatch')

    def read(self, tf):
        return parse_data_window(self.page.get_by_role('treegrid').inner_text(),tf,self.timezone_name)

    def latest(self, tf):
        # Put the cursor outside the plot: the window shows the latest candle.
        self.page.get_by_role('button',name='Change interval',exact=True).hover()
        self.page.wait_for_timeout(150)
        return self.read(tf)

    def associate(self, tf, candidates, roi):
        output=[]
        for candidate in candidates:
            left,top,right,bottom=candidate['bbox']
            x=(left+right)/2; y=(top+bottom)/2
            if left<=roi[0] or top<=roi[1] or right>=roi[2] or bottom>=roi[3]:
                output.append({**candidate,'association_error':'bubble_clipped'})
                continue
            try:
                # Prove that hovering updates the visible candle, rather than
                # accidentally reusing a stale/latest timestamp for every bubble.
                probes=[]
                for probe in (roi[0]+4,roi[2]-4):
                    self.page.mouse.move(probe,y)
                    self.page.wait_for_timeout(150)
                    probes.append(self.read(tf))
                self.page.mouse.move(x,y)
                self.page.wait_for_timeout(150)
                first=self.read(tf)
                self.page.mouse.move(x+1,y)
                self.page.wait_for_timeout(150)
                second=self.read(tf)
                if first!=second or all(probe==first for probe in probes):
                    raise ValueError('candle_boundary_or_loading')
                output.append({**candidate,'candle_start':first,'association_verified':True,
                               'bubble_id':'luxalgo-candle-direction-v1',
                               'identity_scope':'coincident_candle_direction'})
            except Exception:
                output.append({**candidate,'association_error':'data_window_unverified'})
        return output
