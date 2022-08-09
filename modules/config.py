experiment = 'build_metadata'
journey_chunk = 50000
available_executors = 10
from modules.libraries import *
from modules.db_tables import *

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

## Databases and connectors
# MySQL
user, password, db_name = 'rony', 'exp8546$fs', 'MCdb'
server_db_params = {'Local': {'host': 'localhost', 'user': user, 'password': password, 'database': db_name},\
                    'Remote': {'host': serviceIP, 'user': user, 'password': password, 'database': db_name}}
conn_params = server_db_params[serviceLocation]
conn = mysql.connect(**conn_params)
cur = conn.cursor()
cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
resultsIP = '172.31.10.240'
results_params = {'host': resultsIP, 'user': user, 'password': password, 'database': db_name}
results_conn = mysql.connect(**results_params)
results_cur = results_conn.cursor()
results_cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
				.format(host=results_params['host'], db=results_params['database'],\
                        user=results_params['user'], pw=results_params['password']))

chains_cols_types = {'id': 'TEXT', 'chain': 'TEXT'}
chains_cols = list(chains_cols_types.keys())
results_cols_types = {'ID': 'TEXT', 'ChainID': 'TEXT', 'NeighbourID': 'TEXT',\
                      'Dependency': 'TEXT', 'TaskType': 'TEXT', 'Label':  'TEXT',\
                      'PlannedStart': 'TEXT', 'PlannedEnd':  'TEXT', 'ActualStart':  'TEXT', 'ActualEnd':  'TEXT',\
                      'Float':  'DOUBLE', 'Status':  'TEXT', 'File':  'TEXT',\
                      'planned_duration':  'DOUBLE', 'actual_duration':  'DOUBLE'}
chains_table, results_table = '{e}_chains'.format(e=experiment), 'results'

# Redis
redisClient = redis.Redis(host='localhost', port=6379, db=3, decode_responses=True)
successorsDB = redis.Redis(host='localhost', port=6379, db=4, decode_responses=True)

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

sub_graphs_path = os.path.join(data_path, 'sub_graphs')
if 'sub_graphs' not in os.listdir(data_path):
	os.mkdir(sub_graphs_path)