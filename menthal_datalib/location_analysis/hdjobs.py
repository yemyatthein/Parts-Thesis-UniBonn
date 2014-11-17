import sys, math, json
sys.path.append('..')
from mrjob.job import MRJob
from datetime import datetime
from Queue import Queue
from mrjob.protocol import JSONValueProtocol
from ..config import Configuration, DATE_FORMAT, TIME_FORMAT
from core import LocationEventInstance, UserEventProfile
from ..utils import gran_to_func, period_to_hashkey, decode_time

class RefineInputJob(MRJob):
    OUTPUT_PROTOCOL = JSONValueProtocol
    def refine_mapper(self, _, line):
        ls_location_event = line.split(',')
        tokens = line.split(',')
        dt_string = '%s %s' % (tokens[1], tokens[2].split('.')[0].split('+')[0])
        dt_instance = datetime.strptime(dt_string, '%s %s' % (DATE_FORMAT, TIME_FORMAT))
        event = LocationEventInstance(dt_instance, tokens[3], float(tokens[4]), float(tokens[5]), float(tokens[6]))
        for g in self.JOBCONF['TIME_GRANULARITY']:
            firstday, lastday = gran_to_func[g](dt_instance)
            hashkey = period_to_hashkey(tokens[0], firstday, lastday)
            valdict = event.get_dict()
            valdict.update({'username': tokens[0], 'hashkey': hashkey})
            yield None, valdict
    def steps(self):
        return [self.mr(mapper=self.refine_mapper)]

class ProcessRefinedInputJob(MRJob):
    OUTPUT_PROTOCOL = JSONValueProtocol
    def process_mapper(self, _, line):
        lei = json.loads(line)
        hashkey = lei['hashkey']
        yield (hashkey, lei)
    def process_reducer(self, key, values):
        conf = Configuration()
        conf.update(self.JOBCONF)
        username, period = key.split(':')
        start, end = decode_time(*key.split(':')[1].split('|'))
        uep = UserEventProfile(conf, username, start, end, self.analysis_class)
        for x in values:
            dt = datetime.strptime('%s %s' % (x['date'], x['time']), '%s %s' % (DATE_FORMAT, TIME_FORMAT))
            lei = LocationEventInstance(dt, x['ls_provider'], x['accuracy'], x['lat'], x['lng'])
            uep.add_event(lei)
        analysis = uep.get_analysis_object(self.distance_func)
        path_travelled, distance_covered = analysis.get_distance_travelled()
        bbox = analysis.get_bounding_box()
        try:
            crbox = analysis.get_common_region()
        except:
            crbox = []
        output = {'username': uep.username, 'period_start': str(uep.period_start), 'hashkey': key, \
                'period_end': str(uep.period_end), 'path_travelled': path_travelled, \
                'distance_covered': distance_covered, 'bounding_box': bbox, 'common_region': crbox}
        yield (None, output)
    def steps(self):
        if 'analysis_class_mod' in self.JOBCONF:
            amod = __import__(self.JOBCONF['analysis_class_mod'])
            self.analysis_class = getattr(amod, self.JOBCONF['analysis_class_name'])
        else:
            self.analysis_class = None
        if 'distance_func_mod' in self.JOBCONF:
            dmod = __import__(self.JOBCONF['distance_func_mod'])
            self.distance_func = getattr(dmod, self.JOBCONF['distance_func_name'])
        else:
            self.distance_func = None
        return [self.mr(mapper=self.process_mapper, reducer=self.process_reducer)]
