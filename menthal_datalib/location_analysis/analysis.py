import sys, json, os, subprocess
sys.path.append('..')
from datetime import datetime
from ..config import (DATE_FORMAT, TIME_FORMAT, REFINED_INPUT_FILE, FINAL_OUTPUT_FILE)
from core import LocationEventInstance, UserEventProfile
from hdjobs import RefineInputJob, ProcessRefinedInputJob
from ..utils import gran_to_func, period_to_hashkey

DBNAME = 'location_analysis_db'
CLNAME_OUTPUT = 'location_result'
CLNAME_INTERMEDIATE = 'refined_input'

class LocationAnalysisRunner(object):
    def __init__(self, config):
        self.config = config
        self.distance_func = None
        self.analysis_class = None
    def initialize(self):
        pass
    def set_distance_function(self, func):
        self.distance_func = func
    def set_analysis_class(self, cls):
        self.analysis_class = cls
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
        
class InMemoryLocationAnalysisRunner(LocationAnalysisRunner):
    def __init__(self, **kargs):
        super(InMemoryLocationAnalysisRunner, self).__init__(**kargs)
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
        def _add_user_profile(gbld, conf, username, start_date, end_date, key, value):
            if key not in gbld:
                gbld[key] = UserEventProfile(conf, username, start_date, end_date, self.analysis_class)
            gbld[key].add_event(value)
            
        self.user_time = {}
        for i in xrange(len(self.config['INPUT_PATHS'])):
            path = self.config['INPUT_PATHS'][i]
            with open(path, 'r') as f:
                for line_index, line in enumerate(f):
                    if self.config['DEBUG_LIMIT'] and line_index == self.config['DEBUG_LIMIT'] + 1:
                        break
                    tokens = line.split(',')
                    dt_string = '%s %s' % (tokens[1], tokens[2].split('.')[0].split('+')[0])
                    dt_instance = datetime.strptime(dt_string, '%s %s' % (DATE_FORMAT, TIME_FORMAT))
                    event = LocationEventInstance(dt_instance, tokens[3], float(tokens[4]), float(tokens[5]), float(tokens[6]))
                    for g in self.config['TIME_GRANULARITY']:
                        firstday, lastday = gran_to_func[g](dt_instance)
                        hashkey = period_to_hashkey(tokens[0], firstday, lastday)
                        _add_user_profile(self.user_time, self.config, tokens[0], firstday, lastday, hashkey, event)
                    
        with open(self.config['WORKING_DIR'] + '/' + REFINED_INPUT_FILE, 'a') as f:
            for k, v in self.user_time.iteritems():
                for ev in v.get_events_sorted():
                    dct = ev.get_dict()
                    dct.update({'username': v.username, 'hashkey': k})
                    self.refined_inputs.append(dct)
                    json.dump(dct, f)
                    f.write('\n')
    def process_data(self):
        with open(self.config['WORKING_DIR'] + '/' + FINAL_OUTPUT_FILE, 'a') as f:
            for k, udep in self.user_time.iteritems():
                analysis = udep.get_analysis_object(self.distance_func)
                path_travelled, distance_covered = analysis.get_distance_travelled()
                bbox = analysis.get_bounding_box()
                crbox = analysis.get_common_region()
                output = {'username': udep.username, 'period_start': str(udep.period_start), \
                        'period_end': str(udep.period_end), 'path_travelled': path_travelled, 'hashkey': k, \
                        'distance_covered': distance_covered, 'bounding_box': bbox, 'common_region': crbox}
                json.dump(output, f)
                self.analysis_outputs.append(output)
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
        for k, v in self.user_time.iteritems():
            for ev in v.get_events_sorted():
                dct = ev.get_dict()
                dct.update({'username': v.username, 'hashkey': k})
                cl2.insert(dct)
        client.close()
            
class HadoopLocationAnalysisRunner(LocationAnalysisRunner):
    def __init__(self, **kargs):
        super(HadoopLocationAnalysisRunner, self).__init__(**kargs)
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
        if self.analysis_class:
            self.hadoop_jobconf['analysis_class_mod'] = self.analysis_class.__module__
            self.hadoop_jobconf['analysis_class_name'] = self.analysis_class.__name__
        if self.distance_func:
            self.hadoop_jobconf['distance_func_mod'] = self.distance_func.__module__
            self.hadoop_jobconf['distance_func_name'] = self.distance_func.__name__
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
            
class LocationResultQueryManager(object):
    def __init__(self, config):
        self.config = config
    def get_all_results(self):
        from pymongo import MongoClient
        client = MongoClient(self.config['MONGO_DBHOST'], self.config['MONGO_DBPORT'])
        db = client[DBNAME]
        cl = db[CLNAME_OUTPUT]
        rs = []
        for x in cl.find():
            x.pop('_id')
            rs.append(json.loads(json.dumps(x)))
        return rs
    def get_points_for(self, uep):
        from pymongo import MongoClient
        client = MongoClient(self.config['MONGO_DBHOST'], self.config['MONGO_DBPORT'])
        db = client[DBNAME]
        cl = db[CLNAME_INTERMEDIATE]
        hashkey = uep['hashkey']
        ls = [(x['lat'], x['lng']) for x in cl.find({'hashkey': hashkey})]
        return json.loads(json.dumps(ls))
