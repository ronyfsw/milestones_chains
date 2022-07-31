experiment = 'distributed_pipeline_hashed_chains'
new_experiment = True
available_executors = 10
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
if new_experiment:
    redisClient.flushdb()
    successorsDB.flushdb()

# Tables
tracker_cols_types = {'step': 'INTEGER', 'chain_built': 'INTEGER', 'new_chain': 'INTEGER', 'applied_certificates': 'INTEGER', \
                   'chains': 'INTEGER', 'growthD': 'DOUBLE', 'prepD': 'DOUBLE',\
                    'writeD': 'DOUBLE', 'assertD': 'DOUBLE', 'reproduceD': 'DOUBLE', 'updateD': 'DOUBLE', \
                      'processesD': 'DOUBLE', 'stepD': 'DOUBLE', 'step_processes_diff': 'DOUBLE'}
chains_cols_types = {'chain': 'TEXT'}
chains_cols = ['chain']
tracker_table, chains_table = 'tracker_redis', 'chains'

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