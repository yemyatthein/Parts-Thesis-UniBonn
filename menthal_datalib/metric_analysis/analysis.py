import sys, json, os, subprocess
sys.path.append('..')
from datetime import datetime
from core import MetricEventInstance, MetricEventProfile
from hdjobs import RefineInputJob, ProcessRefinedInputJob
from ..config import (DATE_FORMAT, TIME_FORMAT, REFINED_INPUT_FILE, FINAL_OUTPUT_FILE)
from ..utils import period_to_hashkey, gran_to_func

DBNAME = 'metric_analysis_db'
CLNAME_OUTPUT = 'analysis_result'
CLNAME_INTERMEDIATE = 'refined_input'
                
class MetricAnalysisRunner(object):
    def __init__(self, config, calculation_func):
        self.config = config
        self.cal_func = calculation_func
    def initialize(self):
        pass
    def refine_inputcsv(self):
        raise NotImplementedError('To be implemented by subclasses.')
    def process_data(self):
        raise NotImplementedError('To be implemented by subclasses.')
    def run_process(self):
        self.initialize()
        self.refine_inputcsv()
        self.process_data()
    def transfer_to_mongo(self):
        raise NotImplementedError('To be implemented by subclasses.')
        
class InMemoryMetricAnalysisRunner(MetricAnalysisRunner):
    def __init__(self, **kargs):
        super(InMemoryMetricAnalysisRunner, self).__init__(**kargs)
        self.refined_inputs = []
        self.analysis_outputs = []
    def initialize(self):
        if not os.path.exists(self.config['WORKING_DIR']):
            os.makedirs(self.config['WORKING_DIR'])
        else:
            with open(self.config['WORKING_DIR'] + '/' + REFINED_INPUT_FILE, 'w') as f1, \
                open(self.config['WORKING_DIR'] + '/' + FINAL_OUTPUT_FILE, 'w') as f2:
                pass
    def refine_inputcsv(self):
        def _add_user_profile(gbld, conf, username, start_date, end_date, key, value, mname, mtag):
            if key not in gbld:
                gbld[key] = MetricEventProfile(conf, username, start_date, end_date, mname, mtag)
            gbld[key].add_event(value)
        
        self.user_time = {}
        for i in xrange(len(self.config['INPUT_PATHS'])):
            path = self.config['INPUT_PATHS'][i]
            with open(path, 'r') as f:
                for line_index, line in enumerate(f):
                    if self.config['DEBUG_LIMIT'] and line_index == self.config['DEBUG_LIMIT'] + 1:
                        break
                    tokens = line.rstrip().split(',')
                    dt_string = tokens[2].split('.')[0].split('+')[0]
                    dt_instance = datetime.strptime(dt_string, '%s %s' % (DATE_FORMAT, TIME_FORMAT))
                    event = MetricEventInstance(dt_instance, tokens[0], tokens[1], tokens[4], float(tokens[3]))
                    for g in self.config['TIME_GRANULARITY']:
                        firstday, lastday = gran_to_func[g](dt_instance)
                        hashkey = period_to_hashkey(tokens[0], firstday, lastday, additional=[event.tag])
                        _add_user_profile(self.user_time, self.config, tokens[0], firstday, lastday, hashkey, event, \
                                        event.metric_name, event.tag)
                
        with open(self.config['WORKING_DIR'] + '/' + REFINED_INPUT_FILE, 'a') as f:
            for k, v in self.user_time.iteritems():
                dct = v.get_dict()
                vals, times = v.get_series(dateas_str=True)
                dct.update({'hashkey': k, 'values': vals, 'times': times})
                self.refined_inputs.append(dct)
                json.dump(dct, f)
                f.write('\n')
    def process_data(self):
        tdct = {}
        for k, v in self.user_time.iteritems():
            new_key = ':'.join(k.split(':')[:-1])
            if new_key not in tdct:
                tdct[new_key] = []
            tdct[new_key].append(v)
        for k, lsv in tdct.iteritems():
            analysis_objs = [x.get_analysis_object() for x in lsv]
            atp = dict(zip(analysis_objs, lsv))
            for x in analysis_objs:
                dct = {'username': atp[x].username, 'period_start': str(atp[x].period_start), \
                    'period_end': str(atp[x].period_end)}
                if self.config['COMPARE']:
                    for y in analysis_objs:
                        tpl = x.align_data(y)
                        score = self.cal_func(*tpl)
                        dct.update({'from_tag': atp[x].tag, 'to_tag': atp[y].tag, 'score': score})
                else:
                    vals = x.vals
                    result = self.cal_func(vals)
                    dct.update({'tag': atp[x].tag, 'result': result})
                self.analysis_outputs.append(dct)
        with open(self.config['WORKING_DIR'] + '/' + FINAL_OUTPUT_FILE, 'a') as f:
            for r in self.analysis_outputs:
                json.dump(r, f)
                f.write('\n')
    def transfer_to_mongo(self):
        from pymongo import MongoClient
        client = MongoClient(self.config['MONGO_DBHOST'], self.config['MONGO_DBPORT'])
        db = client[DBNAME]
        cl1 = db[CLNAME_OUTPUT]
        cl2 = db[CLNAME_INTERMEDIATE]
        cl1.drop()
        cl2.drop()
        for item in self.analysis_outputs:
            cl1.insert(item)
        for item in self.refined_inputs:
            cl2.insert(item)
        client.close()

class HadoopMetricAnalysisRunner(MetricAnalysisRunner):
    def __init__(self, **kargs):
        super(HadoopMetricAnalysisRunner, self).__init__(**kargs)
        self.hadoop_jobconf = self.config.get_vardict()
        self.intermediate_dir = '%s/%s' % (self.hadoop_jobconf['WORKING_DIR'], REFINED_INPUT_FILE)
        self.final_hadoop_dir = '%s/%s' % (self.hadoop_jobconf['WORKING_DIR'], FINAL_OUTPUT_FILE)
    def refine_inputcsv(self):
        job = RefineInputJob(self.hadoop_jobconf['INPUT_PATHS'] + ['--output-dir', \
                            self.intermediate_dir])
        job.JOBCONF.update(self.hadoop_jobconf)
        with job.make_runner() as runner:
            runner.run()
    def process_data(self):
        self.hadoop_jobconf['cal_func_mod'] = self.cal_func.__module__
        self.hadoop_jobconf['cal_func_name'] = self.cal_func.__name__
        job = ProcessRefinedInputJob([self.intermediate_dir, '--output-dir', \
                            self.final_hadoop_dir])
        job.JOBCONF.update(self.hadoop_jobconf)
        with job.make_runner() as runner:
            runner.run()
    def transfer_to_mongo(self):
        from pymongo import MongoClient
        client = MongoClient(self.config['MONGO_DBHOST'], self.config['MONGO_DBPORT'])
        db = client[DBNAME]
        cl1 = db[CLNAME_OUTPUT]
        cl2 = db[CLNAME_INTERMEDIATE]
        cl1.drop()
        cl2.drop()
        cat = subprocess.Popen(['hadoop', 'fs', '-cat', self.final_hadoop_dir], stdout=subprocess.PIPE)
        for line in cat.stdout:
            jsobj = json.loads(line)
            cl1.insert(jsobj)
        cat = subprocess.Popen(['hadoop', 'fs', '-cat', self.intermediate_dir], stdout=subprocess.PIPE)
        for line in cat.stdout:
            jsobj = json.loads(line)
            cl2.insert(jsobj)
        client.close()

class MetricResultQueryManager(object):
    def __init__(self, config):
        self.config = config
    def get_result(self, from_tag, to_tag, start, end):
        from pymongo import MongoClient
        client = MongoClient(self.config['MONGO_DBHOST'], self.config['MONGO_DBPORT'])
        db = client[DBNAME]
        cl = db[CLNAME_OUTPUT]
        rs = []
        # TODO: Query parsing and nearest period casting
        for x in cl.find():
            print x
            break 