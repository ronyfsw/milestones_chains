from pathlib import Path
import os
import sys
home_dir = Path.home()
modules_dir = os.path.join(home_dir, 'services/milestones_chains/modules/')
if modules_dir not in sys.path: sys.path.append(modules_dir)
from modules.libraries import *

# Pipeline
partition_size_cutoff = 50
journey_chunk = 50000
chains_chunk = 100000
available_executors = 40
node_delimiter = '<>'
chains_file = 'chains.parquet'
num_executors = 40
results_cols = ['ID', 'ChainID', 'TaskID', 'NeighbourID', 'Dependency', 'TaskType', 'Label',
                'PlannedStart', 'PlannedEnd', 'ActualStart', 'ActualEnd', 'Float1', 'Status', 'File', 'planned_duration', 'actual_duration']

## AWS
# EC2 Instances
INSTANCE_IPs = {'services': '172.31.15.123', 'services_dev': '172.31.20.61'}
INSTANCE_IDs = {'service': 'i-0586e11281d4b02a2', 'service_dev': 'i-0249408ea16bc730b'}
AWS_REGION = "eu-west-2"

# S3 Storage
profile_name = 'ds_sandbox'
results_bucket = 'chainsresults'
data_bucket = 'programsdatabucket'
session = boto3.session.Session(profile_name=profile_name)
S3_CLIENT = session.client('s3')
S3_RESOURCE = session.resource('s3')

## Databases and connectors
# MySQL
chains_cols_types = {'id': 'TEXT', 'chain': 'TEXT'}
chains_cols = list(chains_cols_types.keys())
db_user, db_password, db_name = 'rony', 'exp8546$fs', 'MCdb'

# Redis
redisClient = redis.Redis(host='localhost', port=6379, db=3, decode_responses=True)
successorsDB = redis.Redis(host='localhost', port=6379, db=4, decode_responses=True)

# Paths
working_dir = os.getcwd()
data_path = os.path.join(working_dir, 'data')
run_dir_path = os.path.join(working_dir, 'run_dir')
chunks_path = os.path.join(run_dir_path, 'chunks')
sub_graphs_path = os.path.join(run_dir_path, 'sub_graphs')
scaffolds_path = os.path.join(run_dir_path, 'scaffolds')
