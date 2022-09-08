from modules.libraries import *
from modules.parsers import *
from modules.evaluate import *
from modules.graphs import *
from modules.config import *
from modules.db_tables import *
start_time = datetime.now().strftime("%H:%M:%S")
print('build results on', start_time)

# Drop results table if exists
results_cur.execute("DROP TABLE IF EXISTS {db}.{t}".format(db=db_name, t=results_table))
statement = build_create_table_statement(db_name, results_table, results_cols_types)
print(statement)
results_cur.execute(statement)
results_cur.execute('SHOW TABLES FROM MCdb;')
print(results_cur.fetchall())

# Chain results
chains_df = pd.read_sql('SELECT * FROM MCdb.{ct}'.format(ct=chains_table), con=conn)
chains = list(set((chains_df['chain'])))
chains = chains[:100]
print('{n1} chains'.format(n1=len(chains)))

# Tasks and Links
file_path = os.path.join(data_path, data_file_name)
G = build_graph(file_path)
links = G.edges(data=True)
links_types = {}
for link in links: links_types[(link[0], link[1])] = link[2]['Dependency']
tasks_decoder = np.load('nodes_decoder.npy', allow_pickle=True)[()]
metadata_duration = pd.read_excel('metadata_duration.xlsx')
# Tasks to Rows
print('Tasks to Rows split')
def chain_to_rows(index_chain):
	rows = []
	chain_index, chain = index_chain
	# Chain index
	chain_index = 'C{i}'.format(i=str(chain_index + 1))
	tasks = chain.split(node_delimiter)
	tasks = [tasks_decoder[t] for t in tasks]
	for index, task in enumerate(tasks):
		# Task index
		if TDAs_in_results:
			task_index = 'T{i}'.format(i=str(index))
		else:
			task_index = 'M{i}'.format(i=str(index+1))
		task_index = chain_index+task_index

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
		rows.append((task, chain_index, task_index, next_task, pair_edge_type))
	return rows

print('collecting and writing results rows')
indices_chains = []
for index, chain in enumerate(chains):
	indices_chains.append((index, chain))
executor = ProcessPoolExecutor(available_executors)
results_rows = []
rows_count = 0
md_ids = list(metadata_duration['ID'])
start = time.time()
chunk = 10
for chain_rows in executor.map(chain_to_rows, indices_chains):
	for chain_row in chain_rows:
		id = chain_row[0]
		if id in md_ids:
			row_md = list(metadata_duration[metadata_duration['ID'] == id].values[0])[1:]
			chain_row = list(chain_row) + row_md
			chain_row = [str(e) for e in chain_row]
			chain_row = tuple(chain_row)
			results_rows.append(chain_row)
	#print(len(chain_rows), len(results_rows))
	if len(results_rows) >= chunk:
		statement = insert_rows(db_name, results_table, results_cols, results_rows)
		results_cur.execute(statement)
		results_conn.commit()
		rows_count += len(results_rows)
		results_rows = []
		print('writing {c} rows took {t} seconds'.format(c=rows_count, t=round(time.time() - start)))

print('build results started on', start_time)
print('build results ended on', datetime.now().strftime("%H:%M:%S"))
