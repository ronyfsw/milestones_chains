import time

import pandas as pd
import matplotlib.pyplot as plt
from modules.libraries import *
from modules.parsers import *
from modules.evaluate import *
from modules.graphs import *
from modules.config import *
from modules.db_tables import *

copy_file_statement = """LOAD DATA LOCAL INFILE 'results_copy.csv' INTO TABLE {t} 
FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' {rc};""".format(t=results_table, rc=str(tuple(results_cols)).replace("'", ''))
# BY '\n' IGNORE 1 LINES {rc}
print('copy_file_statement:', copy_file_statement)

sql_stmt = 'SET GLOBAL local_infile = TRUE'
engine.execute(sql_stmt)
# sql_stmt = 'SET autocommit=0'
# engine.execute(sql_stmt)
sql_stmt = 'SET bulk_insert_buffer_size =1024*1024*1024*24'
engine.execute(sql_stmt)

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
# chains = chains[:100]
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

	chain_rows = []
	for row in rows:
		id = row[0]
		if id in md_ids:
			row_md = list(metadata_duration[metadata_duration['ID'] == id].values[0])[1:]
			row = list(row) + row_md
			row = [str(e) for e in row]
			row = tuple(row)
			chain_rows.append(row)

	return chain_rows

print('collecting and writing results rows')
start = time.time()
indices_chains = []
for index, chain in enumerate(chains):
	indices_chains.append((index, chain))
print('indices_chains prep duration = {t}'.format(t=time.time()-start))
executor = ProcessPoolExecutor(available_executors)
results_rows = []
rows_count = 0
md_ids = list(metadata_duration['ID'])
chunk = 10000000 #10M
print('iterating chains')
start1 = time.time()
performance = []
parquet_counter = 0
for chain_rows in map(chain_to_rows, indices_chains):
	results_rows += chain_rows
	if len(results_rows) >= chunk:
		parquet_counter += 1
		rows_count += len(results_rows)
		build_dur = round(time.time() - start1)
		print('{c} rows build took {t} seconds'.format(c=rows_count, t=build_dur))
		start = time.time()
		results_rows = pd.DataFrame(results_rows, columns=results_cols)
		results_rows.to_parquet(os.path.join(experiment_path, 'results_copy_{c}.parquet'.format(c=parquet_counter)), index=False)
		print('write parquet took = {t}'.format(t=time.time() - start))
		start = time.time()
		engine.execute(copy_file_statement)
		print('write to table took {t} seconds'.format(t=round(time.time() - start)))
		results_rows = []
		performance.append((rows_count, build_dur, round(rows_count/build_dur)))
		df = pd.DataFrame(performance, columns=['rows_count', 'duration', 'rps'])
		df.to_excel('speed.xlsx', index=False)
		if len(df) > 4:
			x_col, y_col = 'rows_count', 'rps'
			plt.scatter(list(df[x_col]), list(df[y_col]), s=4)  # marker='.'
			plt.xlabel(x_col)
			plt.ylabel(y_col)
			plt.savefig('rps.png')

if results_rows:
	results_rows = pd.DataFrame(results_rows, columns=results_cols)
	results_rows.to_parquet(os.path.join(experiment_path, 'results_copy_{c}.parquet'.format(c=parquet_counter)),
	                        index=False)

results_conn.commit()
print('build results started on', start_time)
print('build results ended on', datetime.now().strftime("%H:%M:%S"))
