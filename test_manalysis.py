from menthal_datalib import (Configuration, compute_correlation, \
                InMemoryMetricAnalysisRunner, HadoopMetricAnalysisRunner, \
                MetricResultQueryManager, DBNAME_METRIC)
                            
memory = True
write_db = False
get_pymongo = False 

conf = Configuration(INPUT_PATHS=['input_manalysis/input_a.csv', 'input_manalysis/input_b.csv'], \
                    WORKING_DIR='output_manalysis')


if memory:
    ma = InMemoryMetricAnalysisRunner(config=conf, \
                        calculation_func=compute_correlation)
else:
    ma = HadoopMetricAnalysisRunner(config=conf, \
                    calculation_func=compute_correlation)    
ma.run_process()

if write_db:
    ma.transfer_to_mongo()
    query = MetricResultQueryManager(conf)
    query.get_result('_a_', '_b_', '2011-09-19', '2011-09-25')
if get_pymongo:
    import pymongo
    from pymongo import MongoClient
    client = MongoClient('localhost', 27017)
    db = client[DBNAME_METRIC]
