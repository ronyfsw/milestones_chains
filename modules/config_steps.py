experiment = 'steps_without_chains'

import os
import sys
from modules.db_tables import *
from modules.libraries import *

wd = os.getcwd()
data_path = os.path.join(wd, 'data')
experiment_id = 3
data_file_name = 'MWH-06-UP#13_FSW_REV.graphml'
file_path = os.path.join(data_path, data_file_name)
partition_size_cutoff = 50

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

# Tables
tracker_table = 'tracker'
tracker_cols_types = {'step': 'INTEGER', 'chain_built': 'INTEGER', 'new_chain': 'INTEGER', 'applied_certificates': 'INTEGER', \
                   'chains': 'INTEGER', 'startD': 'DOUBLE', 'growthD': 'DOUBLE', 'prepD': 'DOUBLE',\
                    'writeD': 'DOUBLE', 'reproduceD': 'DOUBLE', 'updateD': 'DOUBLE', 'certificate_select_duration':'Double',\
                      'processesD': 'DOUBLE', 'stepD': 'DOUBLE', 'step_processes_diff': 'DOUBLE', 'step_processes_diff_ratio': 'DOUBLE'}
ratios_cols = {'start_durationDratio':'DOUBLE', 'growth_durationDratio':'DOUBLE', 'prep_chain_durationDratio':'DOUBLE',\
 'write_durationDratio':'DOUBLE', 'reproduce_durationDratio':'DOUBLE', \
 'update_durationDratio':'DOUBLE', 'certificate_select_duration':'DOUBLE'}
tracker_cols_types = {**tracker_cols_types, **ratios_cols}

chains_cols_types = {'worm': 'INTEGER', 'chain': 'INTEGER', 'nodes': 'TEXT',  'tip': 'TEXT', 'pointer': 'TEXT'}
chains_cols = ['worm', 'chain', 'nodes', 'tip', 'pointer']

tracker_table, chains_table = 'tracker', 'chains'

# Directories
working_dir = os.getcwd()
data_path = os.path.join(os.getcwd(), 'data')
file_path = os.path.join(data_path, data_file_name)
results_path = os.path.join(working_dir, 'results')
#experiment_dir = 'experiment_{id}'.format(id=experiment_id)
source_path = os.path.join(results_path, 'validation', 'predecessors_successors.xlsx')
experiment_path = os.path.join(results_path, experiment)
if experiment not in os.listdir(results_path):
    os.mkdir(experiment_path)
plots_path = os.path.join(experiment_path, 'plots')
if 'plots' not in os.listdir(experiment_path):
    os.mkdir(plots_path)


