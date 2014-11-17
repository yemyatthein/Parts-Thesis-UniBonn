from menthal_datalib import (Configuration, InMemoryLocationAnalysisRunner, \
                HadoopLocationAnalysisRunner, LocationResultQueryManager, \
                haversine, LocationDataAnalysis, DBNAME_LOCATION)

memory = True
write_db = False
get_pymongo = False 

class MyClass(LocationDataAnalysis):
    pass
conf = Configuration(INPUT_PATHS=['input_location/input.csv'], \
                     WORKING_DIR='output_location')
if memory:
    lar = InMemoryLocationAnalysisRunner(config=conf)
else:
    lar = HadoopLocationAnalysisRunner(config=conf)

lar.set_distance_function(haversine)
lar.set_analysis_class(MyClass)
lar.run_process()

if write_db:
    lar.transfer_to_mongo()
    qc = LocationResultQueryManager(conf)
    ls = qc.get_all_results()
    print (ls[0], qc.get_points_for(ls[0]))
if get_pymongo:
    import pymongo
    from pymongo import MongoClient
    client = MongoClient('localhost', 27017)
    db = client[DBNAME_LOCATION]
