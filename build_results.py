import time
from pathlib import Path
import os
import sys
home_dir = Path.home()
modules_dir = os.path.join(home_dir, 'services/milestones_chains/modules/')
if modules_dir not in sys.path: sys.path.append(modules_dir)
from libraries import *
from graphs import *
from config import *
from chains import *

start_time = datetime.now().strftime("%H:%M:%S")
print('build results started on', start_time)

parser = argparse.ArgumentParser()
parser.add_argument('data_file_name')
parser.add_argument('experiment')
parser.add_argument('tasks_types')
parser.add_argument('results')
args = parser.parse_args()
data_file_name = args.data_file_name
experiment = args.experiment
tasks_types = args.tasks_types
results = args.results
print('build results args:', args)
executor = ProcessPoolExecutor(available_executors)

# Chain results
# Tasks and Links
links_types = np.load(os.path.join(run_dir_path, 'links_types.npy'), allow_pickle=True)[()]
nodes_decoder = np.load(os.path.join(run_dir_path, 'nodes_decoder.npy'), allow_pickle=True)[()]

# Chain results
chains_df = pd.read_sql('SELECT * FROM MCdb.{ct}'.format(ct=chains_table), con=conn)
chains = list(set((chains_df['chain'])))
chains = [(c) for c in chains]
encoded_chains = list(set(chains))
print('{n1} unique scaffold_chains prior to filtering'.format(n1=scaffold_chains_count))
print('{n2} unique chains written'.format(n2=len(encoded_chains)))

# Decode chains
print('decode chains')
chains = []
for chain in encoded_chains:
	encoded_tasks = chain.split(node_delimiter)
	tasks = [nodes_decoder[t] for t in encoded_tasks]
	chain = node_delimiter.join(tasks)
	chains.append(chain)
print('chains decoded')

# Write chains to a parquet file
chains_df = pd.DataFrame(chains, columns=['Chain'])
chains_df.to_parquet(chains_file, index=False, compression='gzip')
chains_path = os.path.join(experiment, chains_file)
S3_CLIENT.upload_file(chains_file, results_bucket, chains_path)

if results == 'prt':
	# Tasks metadata
	print('Generate tasks metadata')
	subprocess.run("python3 metadata_duration.py {f} {t}".format(f=data_file_name, t=tasks_types), shell=True)
	print('Generate tasks metadata completed')
	metadata_duration = pd.read_excel('metadata_duration.xlsx')

	# Tasks to Rows
	print('Tasks to Rows split')

	print('collecting and writing results rows')
	start = time.time()
	indices_chains = []
	for index, chain in enumerate(chains):
		indices_chains.append((index, chain))
	chunked_indices_chains = [indices_chains[i:i + chains_chunk] for i in range(0, len(indices_chains), chains_chunk)]
	indexed_chains_chunks = []
	for chunk_index, chunk in enumerate(chunked_indices_chains):
		indexed_chains_chunks.append((chunk_index, chunk, metadata_duration, tasks_types, \
		                              chunks_path, node_delimiter, results_cols, links_types))
	print('indices_chains prep duration = {t}'.format(t=time.time()-start))
	results_rows = []
	rows_count = 0
	print('iterating chains')
	start1 = time.time()
	performance = []
	parquet_counter = 0
	for chunk_rows_count in executor.map(tasks_rows, indexed_chains_chunks):
		rows_count += chunk_rows_count

	# Merge results
	print('combine, zip and upload results')
	file_names, file_paths = os.listdir(chunks_path), {}
	for file_name in file_names:
		file_paths[file_name] = os.path.join(chunks_path, file_name)
	zipped_results_file_name = '{e}_prt.zip'.format(e=experiment)
	with ZipFile(zipped_results_file_name, 'w') as zip:
		for file_name, file_path in file_paths.items():
			zip.write(file_path, arcname=file_name)
	experiment_path = os.path.join(experiment, 'prt')
	S3_CLIENT.upload_file(zipped_results_file_name, results_bucket, experiment_path)

print('build results started on', start_time)
print('build results ended on', datetime.now().strftime("%H:%M:%S"))