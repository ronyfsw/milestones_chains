from pathlib import Path
import os
import sys
home_dir = Path.home()
modules_dir = os.path.join(home_dir, 'services/milestones_chains/modules/')
if modules_dir not in sys.path: sys.path.append(modules_dir)
from libraries import *
from graphs import *
from config import *
from db_tables import *

start_time = datetime.now().strftime("%H:%M:%S")
print('build results started on', start_time)

parser = argparse.ArgumentParser()
parser.add_argument('data_file_name')
parser.add_argument('experiment')
parser.add_argument('tasks_types')
args = parser.parse_args()
data_file_name = args.data_file_name
experiment = args.experiment
tasks_types = args.tasks_types
TDAs_in_results = False
if tasks_types == 'tdas': TDAs_in_results = True
print('build rows args:', args)

# Chain results
chains_table = '{e}_chains'.format(e=experiment)
chains_df = pd.read_sql('SELECT * FROM MCdb.{ct}'.format(ct=chains_table), con=conn)
chains = list(set((chains_df['chain'])))
# chains = chains[:10000]
print('{n1} chains'.format(n1=len(chains)))

# Tasks and Links
links_types = np.load(os.path.join(run_dir_path, 'links_types.npy'), allow_pickle=True)[()]
tasks_decoder = np.load(os.path.join(run_dir_path, 'nodes_decoder.npy'), allow_pickle=True)[()]

# Tasks metadata
print('Generate tasks metadata')
subprocess.run("python3 metadata_duration.py {f} {t}".format(f=data_file_name, t=tasks_types), shell=True)
print('Generate tasks metadata completed')
metadata_duration = pd.read_excel('metadata_duration.xlsx')

# Tasks to Rows
print('Tasks to Rows split')
def chain_to_rows(index_chunk):
	chunk_index, indices_chains = index_chunk
	results_file_name = os.path.join(chunks_path, 'results_copy_{c}.parquet'.format(c=chunk_index))
	rows = []
	for index_chain in indices_chains:
		chain_index, chain = index_chain
		# Chain index
		chain_index = 'C{i}'.format(i=str(chain_index + 1))
		tasks = chain.split(node_delimiter)
		print('tasks:', tasks)
		print('chain:', chain)
		tasks = [tasks_decoder[t] for t in tasks]
		for index, task in enumerate(tasks):
			# Task index
			if TDAs_in_results:
				task_index = 'T{i}'.format(i=str(index+1))
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
	results_rows = pd.DataFrame(chain_rows, columns=results_cols)
	results_rows.to_parquet(results_file_name, index=False, compression='gzip')
	return len(chain_rows)

print('collecting and writing results rows')
start = time.time()
indices_chains = []
for index, chain in enumerate(chains):
	indices_chains.append((index, chain))
chunked_indices_chains = [indices_chains[i:i + chains_chunk] for i in range(0, len(indices_chains), chains_chunk)]
indexed_chains_chunks = []
for index, chunk in enumerate(chunked_indices_chains):
	indexed_chains_chunks.append((index, chunk))
print('indices_chains prep duration = {t}'.format(t=time.time()-start))
executor = ProcessPoolExecutor(available_executors)
results_rows = []
rows_count = 0
md_ids = list(metadata_duration['ID'])
print('iterating chains')
start1 = time.time()
performance = []
parquet_counter = 0
for chunk_rows_count in executor.map(chain_to_rows, indexed_chains_chunks):
	rows_count += chunk_rows_count
	build_dur = round(time.time() - start1, 2)
	print('{c} rows build took {t} seconds'.format(c=rows_count, t=build_dur))
	start = time.time()
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

print('build rows started on', start_time)
print('build rows ended on', datetime.now().strftime("%H:%M:%S"))

# Results file
print('combine, zip and upload results')
#pq.write_table(pq.ParquetDataset(chunks_path).read(), 'results.parquet', row_group_size=100000)
#print('zip results')
#os.system('zip {r} {f}'.format(f='results.parquet', r=zipped_results_file_name))
#s3_client.upload_file(zipped_results_file_name, results_bucket, experiment)

# Tasks metadata
subprocess.run("python3 merge_file.py {e}".format(e=experiment), shell=True)

