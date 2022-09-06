import pandas as pd

from modules.libraries import *
from modules.parsers import *
from modules.evaluate import *
from modules.graphs import *
from modules.config import *
from modules.db_tables import *
start_time = datetime.now().strftime("%H:%M:%S")
print('build results on', start_time)

# Drop results table if exists
# results_cur.execute("DROP TABLE IF EXISTS {db}.{t}".format(db=db_name, t=results_table))
statement = build_create_table_statement(db_name, results_table, results_cols_types)
print(statement)
results_cur.execute(statement)
results_cur.execute('SHOW TABLES FROM MCdb;')
print(results_cur.fetchall())

# Chain results
chains_df = pd.read_sql('SELECT * FROM MCdb.{ct}'.format(ct=chains_table), con=conn)
chains = list(set((chains_df['chain'])))
#chains = chains[:100]
print('{n1} chains'.format(n1=len(chains)))

# Tasks and Links
file_path = os.path.join(data_path, data_file_name)
G = build_graph(file_path)
links = G.edges(data=True)
links_types = {}
for link in links: links_types[(link[0], link[1])] = link[2]['Dependency']
tasks_decoder = np.load('nodes_decoder.npy', allow_pickle=True)[()]
metadata_duration = pd.read_excel('metadata_duration.xlsx')

def chain_to_rows(index_chain):
	rows = []
	chain_index, chain = index_chain
	print(chain_index, chain)
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

print('collecting and writing results rows')
indices_chains = []
for index, chain in enumerate(chains):
	indices_chains.append((index, chain))
executor = ProcessPoolExecutor(available_executors)
rows_count = chains_count = 0
md_ids = list(metadata_duration['ID'])
start = time.time()
chains_rows = []
print('chain_to_rows')
for chain_rows in executor.map(chain_to_rows, indices_chains):
	print('chain_rows:', len(chain_rows), chain_rows)
	chains_count += len(indices_chains)
	chains_rows += chain_rows
	print('chains_rows:', len(chains_rows), chains_rows)
	if len(chains_rows) >= 1000:
		df = pd.DataFrame(chains_rows, columns=['ID', 'ChainID', 'NeighbourID', 'Dependency'])
		rows_count += len(df)
		print('preparing {c1} rows of {c2} chains took {t} seconds'.format(c1=rows_count, c2=chains_count, t=round(time.time() - start)))
		chains_rows = []