import sys, json
sys.path.append('..')
from mrjob.job import MRJob
from datetime import datetime
from mrjob.protocol import JSONValueProtocol, RawValueProtocol
from core import MetricEventInstance, MetricEventProfile
from functools import partial
from ..config import Configuration, DATE_FORMAT, TIME_FORMAT
from ..utils import gran_to_func, period_to_hashkey, decode_time

parse_time = lambda x, has_time=False: \
                datetime.strptime(x if has_time else (x + ' 00:00:00'), \
                '%s %s' % (DATE_FORMAT, TIME_FORMAT))

class RefineInputJob(MRJob):
    OUTPUT_PROTOCOL = JSONValueProtocol
    def refine_mapper(self, _, line):
        ls_location_event = line.split(',')
        tokens = line.rstrip().split(',')
        dt_string = tokens[2].split('.')[0].split('+')[0]
        dt_instance = datetime.strptime(dt_string, '%s %s' % (DATE_FORMAT, TIME_FORMAT))
        event = MetricEventInstance(dt_instance, tokens[0], tokens[1], tokens[4], float(tokens[3]))
        for g in self.JOBCONF['TIME_GRANULARITY']:
            firstday, lastday = gran_to_func[g](dt_instance)
            hashkey = period_to_hashkey(tokens[0], firstday, lastday, additional=[event.tag])
            valdict = event.get_dict()
            valdict.update({'hashkey': hashkey})
            yield hashkey, valdict
    def refine_reducer(self, key, values):
        conf = Configuration()
        conf.update(self.JOBCONF)
        lsval = [x for x in values]
        start, end = decode_time(*key.split(':')[1].split('|'))
        mep = MetricEventProfile(conf, lsval[0]['username'], start, end, lsval[0]['metric_name'], lsval[0]['tag'])
        for x in lsval:
            dtobj = datetime.strptime('%s %s' % (x['date'], x['time']), '%s %s' % (DATE_FORMAT, TIME_FORMAT))
            event = MetricEventInstance(dtobj, x['username'], x['metric_name'], x['tag'], x['value'])
            mep.add_event(event)
        dct = mep.get_dict()
        vals, times = mep.get_series(dateas_str=True)
        dct.update({'hashkey': key, 'values': vals, 'times': times})
        yield key, dct
    def steps(self):
        return [self.mr(mapper=self.refine_mapper, reducer=self.refine_reducer)]

class ProcessRefinedInputJob(MRJob):
    OUTPUT_PROTOCOL = RawValueProtocol
    def process_mapper(self, _, line):
        mei = json.loads(line)
        hashkey = ':'.join(mei['hashkey'].split(':')[:-1])
        yield (hashkey, mei)
    def process_reducer(self, key, values):
        def create_mep(inp):
            times = map(partial(parse_time, has_time=True), inp['times'])
            start, end = decode_time(*key.split(':')[1].split('|'))
            mep = MetricEventProfile(conf, inp['username'], start, end, inp['metric_name'], inp['tag'])
            mep.set_series_direct(inp['values'], times)
            return mep
        conf = Configuration()
        conf.update(self.JOBCONF)
        lsval = [create_mep(x) for x in values]
        analysis_objs = [x.get_analysis_object() for x in lsval]
        atp = dict(zip(analysis_objs, lsval))
        for x in analysis_objs:
            dct = {'username': atp[x].username, 'period_start': str(atp[x].period_start), \
                    'period_end': str(atp[x].period_end)}
            if self.config['COMPARE']:
                for y in analysis_objs:
                    final_val = dct.copy()
                    tpl = x.align_data(y)
                    score = self.cal_func(*tpl)
                    final_val.update({'from_tag': atp[x].tag, 'to_tag': atp[y].tag, 'score': score})
                    yield (None, final_val)
            else:
                final_val = dct.copy()
                vals = x.vals
                result = self.cal_func(vals)
                final_val.update({'tag': atp[x].tag, 'result': result})
                yield (None, final_val)
    def steps(self):
        mod = __import__(self.JOBCONF['cal_func_mod'])
        self.cal_func = getattr(mod, self.JOBCONF['cal_func_name'])
        return [self.mr(mapper=self.process_mapper, reducer=self.process_reducer)]
