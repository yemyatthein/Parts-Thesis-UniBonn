import sys
sys.path.append('..')
from ..contrib import LocationDataAnalysis

class LocationEventInstance(object):
    def __init__(self, datetimeobj, ls_provider, accuracy, lat, lng):
        self.datetimeobj = datetimeobj
        self.date = datetimeobj.date()
        self.time = datetimeobj.time()
        self.ls_provider = ls_provider
        self.accuracy = accuracy
        self.lat = lat
        self.lng = lng
    def __repr__(self):
        return '(lat:%s, lng:%s) at %s %s' % (self.lat, self.lng, self.time, self.date)
    def get_dict(self):
        return {'date': str(self.date), 'time': str(self.time), 'ls_provider': self.ls_provider, \
                'accuracy': self.accuracy, 'lat': self.lat, 'lng': self.lng}

class UserEventProfile(object):
    def __init__(self, config, username, period_start, period_end, analysis_class=None):
        self.config = config
        self.username = username
        self.period_start = period_start
        self.period_end = period_end
        self.analysis_class = analysis_class
        if not analysis_class:
            self.analysis_class = LocationDataAnalysis
        self.ls_events = []
        self.analysis_object = None  # lazy init
    def add_event(self, levent_instance):
        self.ls_events.append(levent_instance)
    def get_events_sorted(self, asc=True):
        return sorted(self.ls_events, key=lambda x: x.datetimeobj, reverse=not asc)
    def __repr__(self):
        return 'Daily Event Profile for %s (Between %s and %s)' % (self.username, self.period_start, self.period_end)
    def get_series(self, asc=True):
        ls = sorted(self.ls_events, key=lambda x: x.datetimeobj, reverse=not asc)
        coordinates = []
        times = []
        for x in ls:
            coordinates.append((x.lat, x.lng))
            times.append(x.datetimeobj)
        return coordinates, times
    def get_analysis_object(self, distance_func=None):
        coords, times = self.get_series()
        return self.analysis_class(self.config, coords, times, self.period_start, self.period_end, distance_func)
