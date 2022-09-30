import os
from modules.config import *
from modules.graphs import *
source_path = '/home/rony/services/milestones_chains/data/EMS_DCMA_DD/predecessor_sueccessors.xlsx'

experiment = None
tasks_types = 'milestones'
results = 'no'
data_file_name = 'EMS_DCMA_DD_23.08.graphml'
if not experiment: experiment = data_file_name.replace('.graphml', '').replace('.', '_')
source = pd.read_excel(source_path)
source_pairs = dict(zip(source['predecessor'], source['successors']))

G = build_graph(data_file_name)
Gedges = list(G.edges())
edges_count = len(Gedges)

chains_df = pd.read_parquet(chains_file)
chains = list(chains_df['Chain'].unique())
chains_count = len(chains_df)
ps_errors = 0
ps_errors = []
for chain in chains:
    tasks = chain.split(node_delimiter)
    for index, task in enumerate(tasks):
        if index < len(tasks)-1:
            chain_successor = tasks[index+1]
            chain_pair = (task, chain_successor)
            #if chain_successor not in source_pairs[task]:
            if chain_pair not in Gedges:
                ps_errors.append((task, chain_successor))
                print(30*'-')
                print((task, chain_successor))
                print(chain)
ps_errors = list(set(ps_errors))
ps_errors_rate = 100 * (len(ps_errors) / edges_count)
print('*** ps errors ***\n', ps_errors)
print('ps_errors_rate:', ps_errors_rate)