from pathlib import Path
import os
import sys
home_dir = Path.home()
modules_dir = os.path.join(home_dir, 'services/milestones_chains/modules/')
if modules_dir not in sys.path: sys.path.append(modules_dir)
from libraries import *

# Data
wd = os.getcwd()
experiment_id = 3
partition_size_cutoff = 50
fill_date = '1944-06-06'

# Pipeline
journey_chunk = 50000
available_executors = 44
node_delimiter = '<>'
chain_delimiter ='<**>'

# Build results
chains_chunk = 100000

# AWS
profile_name = 'ds_sandbox'
results_bucket = 'chainsresults'
data_bucket = 'programsdatabucket'
session = boto3.session.Session(profile_name=profile_name)
s3_client = session.client('s3')
s3_resource = session.resource('s3')
key_file_name = 'ds_eu_west2_2.pem'
resultsIP = '172.31.10.240'

## Server
serviceLocation = 'Local'
num_executors = 6
locationIP = {'Local': '0.0.0.0', 'Remote': '172.31.15.123'}
locationPort = {'Local': 6002, 'Remote': 5000}
serviceIP = locationIP[serviceLocation]
servicePort = locationPort[serviceLocation]
url = 'http://{ip}:{port}/cluster_analysis/api/v0.1/milestones'.format(ip=serviceIP, port=servicePort)

## Databases and connectors
# MySQL
user, password, db_name = 'rony', 'exp8546$fs', 'MCdb'
server_db_params = {'Local': {'host': 'localhost', 'user': user, 'password': password, 'database': db_name},\
                    'Remote': {'host': serviceIP, 'user': user, 'password': password, 'database': db_name}}
conn_params = server_db_params[serviceLocation]
conn = mysql.connect(**conn_params)
cur = conn.cursor()
cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
chains_cols_types = {'id': 'TEXT', 'chain': 'TEXT'}
chains_cols = list(chains_cols_types.keys())
results_cols = ['ID', 'ChainID', 'TaskID', 'NeighbourID', 'Dependency', 'TaskType', 'Label',
                'PlannedStart', 'PlannedEnd', 'ActualStart', 'ActualEnd', 'Float1', 'Status', 'File', 'planned_duration', 'actual_duration']

# Redis
redisClient = redis.Redis(host='localhost', port=6379, db=3, decode_responses=True)
successorsDB = redis.Redis(host='localhost', port=6379, db=4, decode_responses=True)