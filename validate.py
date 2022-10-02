import os
from modules.config import *
from modules.graphs import *

# chains_file arguments
parser = argparse.ArgumentParser()
parser.add_argument('chains_file')
parser.add_argument('data_file_name')
args = parser.parse_args()
chains_file = args.chains_file
data_file_name = os.path.join(data_path, args.data_file_name)
print('Data:', data_file_name)
print('chains_file:', chains_file)

# Data
G = build_graph(data_file_name)
Gedges = list(G.edges())
edges_count = len(Gedges)
isolates = graph_isolates(G)
terminal_nodes = get_terminal_nodes(G)
root_node = list(nx.topological_sort(G))[0]
Gnodes, Gedges = list(G.nodes()), list(G.edges())
print('Graph with {n} nodes and {e} edges'.format(n=len(Gnodes), e=len(Gedges)))
print('{n} terminal nodes'.format(n=len(terminal_nodes)))
print('{n} isolates'.format(n=len(isolates)))
print('First milestone:', root_node)
for node in terminal_nodes:
    terminal_predecessors = list(G.predecessors(node))
    print(node, len(terminal_predecessors), terminal_predecessors)

# Chain results
chains_df = pd.read_parquet(chains_file)
chains = list(chains_df['Chain'].unique())
chains_count = len(chains_df)
print('{n} chains identified'.format(n=len(isolates)))

## Validation
print('-----------------------')
print('Root and Terminal Nodes')
root_errors = terminal_errors = 0
chains_terminals = error_terminals = []
for chain in chains:
    tasks = chain.split(node_delimiter)
    chain_root, chain_terminal = tasks[0], tasks[-1]
    chains_terminals.append(chain_terminal)
    if chain_root != root_node: root_errors += 1
    if chain_terminal not in terminal_nodes:
        terminal_errors += 1
        error_terminals.append(chain_terminal)
chains_count = len(chains)
root_errors_rate = 100 * (root_errors / chains_count)
terminal_errors_rate = 100 * (terminal_errors / chains_count)
print('{e} chains ({p}%) do not start with the first milestone of the program'.format(e=root_errors, p=root_errors_rate))
print('{e} chains ({p}%) do not end with a terminal task of the program'.format(e=terminal_errors, p=terminal_errors_rate))
if terminal_errors >0: print('The terminal node errors are:', error_terminals)
missing_terminals = list(set(terminal_nodes).difference(set(chains_terminals)))
chains_terminals = list(set(chains_terminals))
print('{n} chains terminals:'.format(n=len(chains_terminals)), chains_terminals)
print('{n} missing terminals:'.format(n=len(missing_terminals)), missing_terminals)

print('----------------------------')
print('Successors-Predecessor Pairs')
ps_errors = pairs_count = 0
ps_errors = []
for chain in chains:
    tasks = chain.split(node_delimiter)
    for index, task in enumerate(tasks):
        if index < len(tasks)-1:
            chain_successor = tasks[index+1]
            chain_pair = (task, chain_successor)
            pairs_count += 1
            if chain_pair not in Gedges:
                ps_errors.append((task, chain_successor))
                # print(30*'-')
                # print((task, chain_successor))
                # print(chain)
ps_errors = list(set(ps_errors))
ps_errors_count = len(ps_errors)
ps_errors_rate = 100 * (ps_errors_count / pairs_count)
print('{e} task pairs in the chains, comprising ({p}%) of the tasks pairs, are not paired as successor-predecessor in the data'
      .format(e=ps_errors_count, p=ps_errors_rate))
if ps_errors_count > 0: print('The error pairs are:', ps_errors)

# for Gedge in Gedges:

