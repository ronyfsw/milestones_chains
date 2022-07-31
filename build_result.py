import pandas as pd

from modules.libraries import *
from modules.parsers import *
from modules.evaluate import *
from modules.graphs import *
from modules.worm_modules import *
from modules.config import *

# Data
G = build_graph(file_path)
root_node = list(nx.topological_sort(G))[0]
terminal_nodes = get_terminal_nodes(G)

# Chain results
chains_df = pd.read_sql('SELECT * FROM {t}'.format(t=chains_table), con=conn).drop_duplicates(subset='nodes')
print(chains_df.info())
chains = list(chains_df['nodes'])
chains_str = '\n'.join(chains)
chains_printout_path = os.path.join(experiment_path, 'chains.txt')
with open(chains_printout_path, 'w') as f: f.write(chains_str)
n1, n2 = len(chains), len(set(chains))
print('{n1} chains | {n2} unique chains'.format(n1=n1, n2=n2))
a=0
# Chains metadata
Gedges = G.edges(data=True)
edges_types = {}
for Gedge in Gedges: edges_types[(Gedge[0], Gedge[1])] = Gedge[2]['Dependency']
nodes_chains = []
for index, row in chains_df.iterrows():
	chain_id = row['chain']
	nodes = row['nodes'].split('<>')
	for index, node in enumerate(nodes):
		if index <= (len(nodes)-2):
			next_node = nodes[index+1]
		else:
			next_node = None
		if next_node:
			try:
				pair_edge_type = edges_types[(node, next_node)]
			except KeyError:
				pair_edge_type = None
		else: pair_edge_type = None
		nodes_chains.append((node, chain_id, next_node, pair_edge_type))
		a = 0
nodes_chains = pd.DataFrame(nodes_chains, columns=['ID', 'ChainID', 'NeighbourID', 'Dependency'])

# Data parse and duration
graphml_str = open(file_path).read().replace('&amp;', '')
headers = ['ID', 'TaskType', 'Label', 'PlannedStart', 'PlannedEnd', 'ActualStart', 'ActualEnd', 'Float', 'Status']
data_df = parse_graphml(data_file_name, graphml_str, headers)
planned_duration = activities_duration(data_df, 'planned')
planned_duration_df = pd.DataFrame(list(zip(list(planned_duration.keys()), list(planned_duration.values()))), columns=['ID', 'planned_duration'])
actual_duration = activities_duration(data_df, 'actual')
actual_duration_df = pd.DataFrame(list(zip(list(actual_duration.keys()), list(actual_duration.values()))), columns=['ID', 'actual_duration'])
planned_actual_df = pd.merge(planned_duration_df, actual_duration_df, how='left')
data_duration = pd.merge(data_df, planned_actual_df)
data_chains_duration = pd.merge(nodes_chains, data_duration, how='left')
#data_chains_duration.to_pickle(os.path.join(experiment_path, 'data_chains_duration.pkl'))
data_chains_duration.to_csv(os.path.join(experiment_path, 'data_chains_duration.csv'), index=False)