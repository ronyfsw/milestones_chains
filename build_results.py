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
from db_tables import *
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

# Chain results
# Tasks and Links
links_types = np.load(os.path.join(run_dir_path, 'links_types.npy'), allow_pickle=True)[()]
nodes_decoder = np.load(os.path.join(run_dir_path, 'nodes_decoder.npy'), allow_pickle=True)[()]

# Chains from scaffolds
chains = []
scaffolds_files = os.listdir(scaffolds_path)
scaffolds = {}
filtering_dir = os.path.join(run_dir_path, 'filtering')
os.mkdir(filtering_dir)
scaffold_chains_count = 0
for scaffolds_file in scaffolds_files:
    start = time.time()
    scaffold_path = os.path.join(scaffolds_path, scaffolds_file)
    scaffold = np.load(scaffold_path, allow_pickle=True)[()]
    scaffold_chains = list(scaffold.values())
    scaffold_chains = list(set([c for c in scaffold_chains if c]))
    scaffold_chains_count += len(scaffold_chains)
    print('filtering {f} with {n} chains'.format(f=scaffolds_file, n=len(scaffold_chains)))
    chains_to_keep = drop_chain_overlaps(scaffold_chains)
    print('{s}: {n1} of {n2} chains kept'.format(s=scaffolds_file,\
                                                 n1=len(chains_to_keep), n2=len(scaffold_chains)))
    print('filtering took', round(time.time()-start))
    result = 'Scaffold chains:\n{sc}\n--------\nChains to keep:\n{ck}'\
	    .format(sc='\n'.join(scaffold_chains), ck='\n'.join(chains_to_keep))
    test_path = os.path.join(filtering_dir, 'test_{f}.txt'.format(f=scaffolds_file.split('_')[1]))
    with open(test_path, 'w') as f: f.write(result)
    chains += chains_to_keep
chains = [(c) for c in chains]
a = len(chains)
encoded_chains = list(set(chains))
print('{n1} unique scaffold_chains prior to filtering'.format(n1=len(scaffold_chains)))
print('{n1} chains identified, {n2} unique chains written'.format(n1=a, n2=len(chains)))

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
	md_ids = list(metadata_duration['ID'])
	print('{n} ids in metadata_duration'.format(n=len(md_ids)))

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
			#print('chain:', chain)
			tasks = chain.split(node_delimiter)
			#rint('tasks:', tasks)
			# chain: M
			# tasks: ['M']
			# tasks = [nodes_decoder[t] for t in tasks]
			for index, task in enumerate(tasks):
				# Task index
				if tasks_types == 'tdas':
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
				row = tuple([str(e) for e in row])
				chain_rows.append(row)

		results_rows = pd.DataFrame(chain_rows, columns=results_cols).drop_duplicates()
		results_rows.to_parquet(results_file_name, index=False, compression='gzip')
		return len(chain_rows)

	print('collecting and writing results rows')
	start = time.time()
	indices_chains = []
	for index, chain in enumerate(chains):
		indices_chains.append((index, chain))
	#print('indices_chains:', indices_chains[:5])
	chunked_indices_chains = [indices_chains[i:i + chains_chunk] for i in range(0, len(indices_chains), chains_chunk)]
	indexed_chains_chunks = []
	for index, chunk in enumerate(chunked_indices_chains):
		indexed_chains_chunks.append((index, chunk))
	print('indices_chains prep duration = {t}'.format(t=time.time()-start))
	executor = ProcessPoolExecutor(available_executors)
	results_rows = []
	rows_count = 0
	print('iterating chains')
	start1 = time.time()
	performance = []
	parquet_counter = 0
	for chunk_rows_count in executor.map(chain_to_rows, indexed_chains_chunks):
		rows_count += chunk_rows_count

	# Merge results
	print('combine, zip and upload results')
	subprocess.run("python3 merge_file.py {e}".format(e=experiment), shell=True)

print('build results started on', start_time)
print('build results ended on', datetime.now().strftime("%H:%M:%S"))


