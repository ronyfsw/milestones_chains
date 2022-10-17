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
# AWS
profile_name = 'ds_sandbox'
results_bucket = 'chainsresults'
data_bucket = 'programsdatabucket'
session = boto3.session.Session(profile_name=profile_name)
S3_CLIENT = session.client('s3')
S3_RESOURCE = session.resource('s3')

# Paths
working_dir = os.getcwd()
data_path = os.path.join(working_dir, 'data')
run_dir_path = os.path.join(working_dir, 'run_dir')
chunks_path = os.path.join(run_dir_path, 'chunks')
sub_graphs_path = os.path.join(run_dir_path, 'sub_graphs')
scaffolds_path = os.path.join(run_dir_path, 'scaffolds')

## Databases and connectors
# Redis
redisClient = redis.Redis(host='localhost', port=6379, db=3, decode_responses=True)
successorsDB = redis.Redis(host='localhost', port=6379, db=4, decode_responses=True)