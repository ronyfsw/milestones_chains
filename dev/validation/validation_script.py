# Validate that the chains produced end in a terminal node
from modules.graphs import *
from modules.worm_modules import *
from modules.config import *

print('### Validation of Results: Chains')
# data
G = build_graph(file_path)
root_node = list(nx.topological_sort(G))[0]
Gnodes, Gedges = list(G.nodes()), G.edges()
terminal_nodes = get_terminal_nodes(G)
tasks_decoder = np.load(os.path.join(run_dir_path, 'nodes_decoder.npy'), allow_pickle=True)[()]
# Chain results
chains_df = pd.read_sql('SELECT * FROM MCdb.{ct}'.format(ct=chains_table), con=conn)
chains = list(set((chains_df['chain'])))
print('{n1} chains'.format(n1=len(chains)))

root_errors = terminal_errors = 0
for chain in chains:
    tasks = chain.split(node_delimiter)
    tasks = [tasks_decoder[t] for t in tasks]
    chain_root, chain_terminal = tasks[0], tasks[-1]
    if chain_root != root_node: root_errors+=1
    if chain_terminal not in terminal_nodes: terminal_errors += 1

chains_count = len(chains)
root_errors_rate = 100*(root_errors/chains_count)
terminal_errors_rate = 100*(terminal_errors/chains_count)
print('root errors rate=', root_errors_rate)
print('terminal errors rate=', terminal_errors_rate)

print('### Validation of Results: Rows')

