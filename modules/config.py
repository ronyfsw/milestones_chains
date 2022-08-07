experiment = 'scaffolds_query'
new_experiment = False
journey_chunk = 50000
available_executors = 14
import os
import sys
import redis
from modules.db_tables import *
from modules.libraries import *

wd = os.getcwd()
data_path = os.path.join(wd, 'data')
experiment_id = 3
data_file_name = 'MWH-06-UP#13_FSW_REV.graphml'
file_path = os.path.join(data_path, data_file_name)
partition_size_cutoff = 50

# Pipeline
node_delimiter = '<>'
chain_delimiter ='<**>'

## Server
serviceLocation = 'Local'
num_executors = 6
locationIP = {'Local': '0.0.0.0', 'Remote': '172.31.15.123'}
locationPort = {'Local': 6002, 'Remote': 5000}
serviceIP = locationIP[serviceLocation]
servicePort = locationPort[serviceLocation]
url = 'http://{ip}:{port}/cluster_analysis/api/v0.1/milestones'.format(ip=serviceIP, port=servicePort)

# Database connection
#server_db_params = {'Local': {'host': 'localhost', 'user': 'rony', 'password': 'exp8546$fs', 'database': db_name},\
#                    'Remote': {'host': serviceIP, 'user': 'researchUIuser', 'password': 'query1234$fs', 'database': db_name}}
#from sqlalchemy import create_engine
import mysql.connector as mysql
private_serviceIP = '172.31.15.123'
user, password, db_name = 'rony', 'exp8546$fs', 'MCdb'
# engine = create_engine('mysql+mysqldb://{u}:{p}@localhost/{db}'\
#                          .format(u=user, p=password, db=db_name))
server_db_params = {'Local': {'host': 'localhost', 'user': user, 'password': password, 'database': db_name},\
                    'Remote': {'host': private_serviceIP, 'user': user, 'password': password, 'database': db_name}}
conn_params = server_db_params[serviceLocation]
conn = mysql.connect(**conn_params)
c = conn.cursor()
c.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")

# Redis
redisClient = redis.Redis(host='localhost', port=6379, db=3, decode_responses=True)
successorsDB = redis.Redis(host='localhost', port=6379, db=4, decode_responses=True)
predecessorsDB = redis.Redis(host='localhost', port=6379, db=5, decode_responses=True)

# Tables
tracker_cols_types ={'journey': 'INTEGER', 'next_count': 'INTEGER', 'scaffolds_count': 'INTEGER',\
 			     'journey_chains_count': 'INTEGER', 'chains_written_count': 'INTEGER',\
	                     'overlap_count': 'INTEGER', 'grow_reproduced': 'DOUBLE', 'scaffolds_queriesd':'DOUBLE',\
                        'unique_idsd': 'DOUBLE',\
	                     'write_scaffoldsd': 'DOUBLE', 'update_mapsd': 'DOUBLE',\
	                     'write_chainsd': 'DOUBLE', 'next_stepsd': 'DOUBLE', 'journeyd': 'DOUBLE'}
chains_cols_types = {'id': 'TEXT', 'chain': 'TEXT'}
chains_cols = list(chains_cols_types.keys())
tracker_table, chains_table = 'tracker', 'chains'

# Build databases and tables
if new_experiment:
    redisClient.flushdb()
    successorsDB.flushdb()
    predecessorsDB.flushdb()
    c.execute("DROP TABLE IF EXISTS {t}".format(t=chains_table))
    statement = build_create_table_statement('{t}'.format(t=chains_table), chains_cols_types)
    c.execute(statement)
    c.execute("DROP TABLE IF EXISTS {t}".format(t=tracker_table))
    statement = build_create_table_statement('{t}'.format(t=tracker_table), tracker_cols_types)
    c.execute(statement)

# Directories
working_dir = os.getcwd()
data_path = os.path.join(os.getcwd(), 'data')
file_path = os.path.join(data_path, data_file_name)
results_path = os.path.join(working_dir, 'results')
#experiment_dir = 'experiment_{id}'.format(id=experiment_id)
source_path = os.path.join(data_path, 'predecessors_successors.xlsx')
experiment_path = os.path.join(results_path, experiment)
if experiment not in os.listdir(results_path):
    os.mkdir(experiment_path)
plots_path = os.path.join(experiment_path, 'plots')
if 'plots' not in os.listdir(experiment_path):
    os.mkdir(plots_path)

chunks_path = os.path.join(experiment_path, 'chunks')
if 'chunks' not in os.listdir(experiment_path):
	os.mkdir(chunks_path)