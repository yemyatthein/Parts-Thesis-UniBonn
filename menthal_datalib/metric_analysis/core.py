import pandas as pd
import numpy as np

class MetricEventInstance(object):
    def __init__(self, datetimeobj, username, metric_name, tag, value):
        self.datetimeobj = datetimeobj
        self.date = datetimeobj.date()
        self.time = datetimeobj.time()
        self.username = username
        self.metric_name = metric_name
        self.tag = tag
        self.value = value
    def __repr__(self):
        return '%s - %s' % (self.__class__.__name__, str(self.get_dict()))
    def get_dict(self):
        return {'date': str(self.date), 'time': str(self.time), 'username': self.username, \
                'metric_name': self.metric_name, 'tag': self.tag, 'value': self.value}

class MetricEventProfile(object):
    def __init__(self, config, username, period_start, period_end, metric_name, tag):
        self.config = config
        self.username = username
        self.period_start = period_start
        self.period_end = period_end
        self.metric_name = metric_name
        self.tag = tag
        self.metric_events = []
        self.analysis_object = None  # lazy instantiation
        self.vals = self.times = None
    def get_dict(self):
        return {'username': self.username, 'period_start': str(self.period_start), 'period_end': str(self.period_end),
                'metric_name': self.metric_name, 'tag': self.tag}
    def add_event(self, mevent_instance):
        self.metric_events.append(mevent_instance)
    def get_events_sorted(self, asc=True):
        return sorted(self.metric_events, key=lambda x: x.datetimeobj, reverse=not asc)
    def __repr__(self):
        return '%s - %s' % (self.__class__.__name__, str(self.get_dict()))
    def get_series(self, dateas_str=False, asc=True):
        ls = self.get_events_sorted(asc)
        if self.vals and self.times:
            return self.vals, self.times
        vals = []
        times = []
        for x in ls:
            vals.append(x.value)
            times.append(str(x.datetimeobj) if dateas_str else x.datetimeobj)
        return vals, times
    def set_series_direct(self, vals, times):
        self.vals = vals
        self.times = times
    def get_analysis_object(self):
        vals, times = self.get_series()
        return MetricDataAnalysis(self.config, vals, times)

class MetricDataAnalysis(object):
    def __init__(self, config, vals, times):
        self.config = config
        self.vals = vals
        self.times = times
    def align_data(self, mda):
        current = dict(zip([x.replace(minute=0, second=0) for x in self.times], self.vals))
        target = dict(zip([x.replace(minute=0, second=0) for x in mda.times], mda.vals))
        earliest = min(sorted(current.keys())[0], sorted(target.keys())[0])
        latest = max(sorted(current.keys())[-1], sorted(target.keys())[-1])
        drange = pd.date_range(earliest, latest, freq='H')
        df = pd.DataFrame(np.zeros([len(drange), 2]), index=drange, columns=['current', 'target'])
        for ind in df.index:
            df['current'][ind] = current.get(ind, 0.0)
            df['target'][ind] = target.get(ind, 0.0)
        return df.index.tolist(), df['current'].tolist(), df['target'].tolist()
