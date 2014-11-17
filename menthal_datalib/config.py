import re

# Keys/Constants
DATE_FORMAT = '%Y-%m-%d'
TIME_FORMAT = '%H:%M:%S'
REFINED_INPUT_FILE = 'refined_input'
FINAL_OUTPUT_FILE = 'output'

class Configuration(object):
    def __init__(self, **entries):
        defaults = {
                    'DEBUG_LIMIT': None,
                    'TIME_GRANULARITY': 'w',
                    'MONGO_DBHOST': 'localhost',
                    'MONGO_DBPORT': 27017,
                    'COMPARE': True,
                    }
        filtered_entries = {}
        for k, v in entries.iteritems():
            if k in defaults and v is None:
                continue
            else:
                filtered_entries[k] = v
        self._e = defaults
        self._e.update(filtered_entries)
        neg = re.findall('[^(h|d|w|m|y)]', self._e['TIME_GRANULARITY'])
        if neg:
            raise RuntimeError('TIME_GRANULARITY accepts only one or more letter from "hdwmy"')
        if len(self._e['TIME_GRANULARITY'].strip()) == 0:
            self._e['TIME_GRANULARITY'] = 'h'
    def __getitem__(self, name):
        return self._e[name] if name in self._e else None
    def update(self, dentries):
        self._e.update(dentries)
    def get_vardict(self):
        return self._e
