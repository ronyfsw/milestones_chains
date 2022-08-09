import sys

import pandas as pd

from modules.libraries import *
from modules.parsers import *
from modules.evaluate import *
from modules.graphs import *
from modules.config import *

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

# Tasks duration
planned_duration = activities_duration(data_df, 'planned')
planned_duration_df = pd.DataFrame(list(zip(list(planned_duration.keys()), list(planned_duration.values()))), columns=['ID', 'planned_duration'])
actual_duration = activities_duration(data_df, 'actual')
actual_duration_df = pd.DataFrame(list(zip(list(actual_duration.keys()), list(actual_duration.values()))), columns=['ID', 'actual_duration'])
planned_actual_df = pd.merge(planned_duration_df, actual_duration_df, how='left')
tasks_duration = pd.merge(data_df, planned_actual_df)

# Chain results
chains_path = os.path.join(experiment_path, 'chains.txt')
chains = open(chains_path).read().split('\n')
n1, n2 = len(chains), len(set(chains))
print('{n1} chains | {n2} unique chains'.format(n1=n1, n2=n2))
a = 0

# Tasks to Rows
print('Tasks to Rows split')
tasks_chains = []
for index, chain in enumerate(chains):
	chain_id = 'C{i}'.format(i=str(index+1))
	tasks = chain.split(node_delimiter)
	tasks = [tasks_decoder[t] for t in tasks]
	for index, task in enumerate(tasks):
		if index <= (len(tasks)-2):
			next_task = tasks[index+1]
		else:
			next_task = None
		if next_task:
			try:
				pair_edge_type = links_types[(task, next_task)]
			except KeyError:
				pair_edge_type = None
		else: pair_edge_type = None
		tasks_chains.append((task, chain_id, next_task, pair_edge_type))
		a = 0
tasks_chains = pd.DataFrame(tasks_chains, columns=['ID', 'ChainID', 'NeighbourID', 'Dependency'])

# Tasks, Metadata and Duration
data_chains_duration = pd.merge(tasks_chains, tasks_duration, how='left')

print('Write chains tasks with metadata')
# Chunk the results to enable their writing and analysis to a spreadsheet
chunk_size = 70
chains_ids = list(tasks_chains['ChainID'].unique())
ids_chunks = [list(c) for c in np.array_split(chains_ids, chunk_size)]
chunks_sizes = []
print('writing results in chunks')
for ids_chunk in ids_chunks:
	chunk = data_chains_duration[data_chains_duration['ChainID'].isin(ids_chunk)]
	chunk_ids = [int(id.replace('C', '')) for id in list(chunk['ChainID'])]
	min_id, max_id = min(chunk_ids), max(chunk_ids)
	file_name = '{m1}_{m2}.xlsx'.format(m1=str(min_id), m2=str(max_id))
	chunk.to_excel(os.path.join(chunks_path, file_name), index=False)
	chunks_sizes.append(sys.getsizeof(chunk))
	print(file_name)
mean_chunk_size = round(np.mean(np.array([sys.getsizeof(c) for c in chunks_sizes]))/(1024 * 1024), 6)
print('mean_chunk_size:', mean_chunk_size)