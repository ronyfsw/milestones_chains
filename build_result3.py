from modules.libraries import *
from modules.parsers import *
from modules.evaluate import *
from modules.graphs import *
from modules.config import *
from modules.db_tables import *
start_time = datetime.now().strftime("%H:%M:%S")
print('pipeline started on', start_time)

# Drop results table if exists
results_cur.execute("DROP TABLE IF EXISTS {db}.{t}".format(db=db_name, t=results_table))
statement = build_create_table_statement(db_name, results_table, results_cols_types)
print(statement)
results_cur.execute(statement)

# Data
file_path = os.path.join(data_path, data_file_name)
G = build_graph(file_path)

# Tasks and Links
links = G.edges(data=True)
links_types = {}
for link in links: links_types[(link[0], link[1])] = link[2]['Dependency']
tasks_decoder = np.load('nodes_decoder.npy', allow_pickle=True)[()]

# Tasks metadata
graphml_str = open(file_path).read().replace('&amp;', '')
headers = ['ID', 'TaskType', 'Label', 'PlannedStart', 'PlannedEnd', 'ActualStart', 'ActualEnd', 'Float', 'Status']
data_df = parse_graphml(data_file_name, graphml_str, headers)
data_df = data_df.rename(columns={'Float':'Float1'})

# Tasks duration
planned_duration = activities_duration(data_df, 'planned')
planned_duration_df = pd.DataFrame(list(zip(list(planned_duration.keys()), list(planned_duration.values()))), columns=['ID', 'planned_duration'])
actual_duration = activities_duration(data_df, 'actual')
actual_duration_df = pd.DataFrame(list(zip(list(actual_duration.keys()), list(actual_duration.values()))), columns=['ID', 'actual_duration'])
planned_actual_df = pd.merge(planned_duration_df, actual_duration_df, how='left')
tasks_duration = pd.merge(data_df, planned_actual_df)

# Chain results
chains_df = pd.read_sql('SELECT * FROM MCdb.{ct}'.format(ct=chains_table), con=conn)
chains = list(set((chains_df['chain'])))
print('{n1} chains'.format(n1=len(chains)))
a = 0

# chains = chains[:100]

# Tasks to Rows
print('Tasks to Rows split')

def chain_to_rows(index_chain):
	rows = []
	chain_index, chain = index_chain
	chain_index = 'C{i}'.format(i=str(chain_index + 1))
	tasks = chain.split(node_delimiter)
	tasks = [tasks_decoder[t] for t in tasks]
	for index, task in enumerate(tasks):
		if index <= (len(tasks) - 2):
			next_task = tasks[index + 1]
		else:
			next_task = None
		if next_task:
			try:
				pair_edge_type = links_types[(task, next_task)]
			except KeyError:
				pair_edge_type = None
		else:
			pair_edge_type = None
		rows.append((task, chain_index, next_task, pair_edge_type))
	return rows

start = time.time()
indices_chains = []
for index, chain in enumerate(chains): indices_chains.append((index, chain))
executor = ProcessPoolExecutor(available_executors)
chains_rows = []
for chain_rows in executor.map(chain_to_rows, indices_chains):
	chains_rows += chain_rows
	a = 0
chains_rows_df = pd.DataFrame(chains_rows, columns=['ID', 'ChainID', 'NeighbourID', 'Dependency'])
print('{n1} chains to {n2} rows extension took {t} seconds'.format(n1=len(indices_chains), n2=len(chains_rows_df), t=round(time.time() - start)))

# Tasks, Metadata and Duration
data_chains_duration = pd.merge(chains_rows_df, tasks_duration, how='left')
print(data_chains_duration.info())
print('Write chains tasks with metadata')
write_chunk = 10000
start = time.time()
rows_count = 0
while len(data_chains_duration) > 0:
	print('{n} rows to write'.format(n=len(data_chains_duration)))
	rows_to_write = data_chains_duration[:write_chunk]
	rows_count += len(rows_to_write)
	print('writing {n1} rows'.format(n1=len(rows_to_write)))
	rows_to_write.to_sql(results_table, engine, index=False, if_exists='append')
	print('writing {c} took {t} seconds'.format(c=rows_count, t=round(time.time()-start)))
	results_conn.commit()
	data_chains_duration = data_chains_duration[write_chunk:]
md_df = pd.read_sql('SELECT * FROM {db}.{rt}'.format(db=db_name, rt=results_table), con=results_conn)
print(md_df.head())
print(md_df.info())
print('pipeline started on', start_time)
print('pipeline ended on', datetime.now().strftime("%H:%M:%S"))