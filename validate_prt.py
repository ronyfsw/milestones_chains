import os, sys, pathlib

import pandas as pd

modules_dir = os.path.join(pathlib.Path.home(), 'services/milestones_chains/modules/')
if modules_dir not in sys.path: sys.path.append(modules_dir)
from graphs import *
from worm_modules import *

# data
file_path = 'EMS_DCMA_DD_23.08.graphml'
G = build_graph(file_path)
root_node = list(nx.topological_sort(G))[0]
Gnodes, Gedges = list(G.nodes()), G.edges()
terminal_nodes = get_terminal_nodes(G)
results_path = '/home/rony/services/milestones_chains/EMS_DCMA_DD_23_08/results_copy_0.parquet'
results = pd.read_parquet(results_path)
results_ids = results[['ID', 'ChainID']]
chain_ids = list(set(results['ChainID']))
chains_count = len(chain_ids)
print('{n} chains'.format(n=chains_count))
print('{n} terminal nodes'.format(n=len(terminal_nodes)))
error_terminals = []
terminal_errors = 0
terminal_errors_df = pd.DataFrame()
for id in chain_ids:
    chain_df = results[results_ids['ChainID'] == id]
    chain_terminal = list(chain_df['ID'])[-1]
    if chain_terminal not in terminal_nodes:
        terminal_errors += 1
        error_terminals.append(chain_terminal)
        terminal_row = chain_df[chain_df['ID'] == chain_terminal]
        terminal_errors_df = pd.concat([terminal_errors_df, terminal_row])
terminal_errors_rate = 100*(terminal_errors/chains_count)
print('terminal errors rate=', terminal_errors_rate)
print('{n} error terminals:'.format(n=len(error_terminals)), error_terminals)
terminal_errors_df.to_excel('terminal_errors_df.xlsx')
