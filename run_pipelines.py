import time
import subprocess

from modules.config import *
from modules.libraries import *
from modules.graphs import *
from modules.chains import *
from modules.encoders import *
from modules.nodes import *
from modules.filters import *
from modules.worm_modules import *
from pipeline1 import *
import warnings
warnings.filterwarnings("ignore")

# Refresh results tables and databases
redisClient.flushdb()
successorsDB.flushdb()
predecessorsDB.flushdb()
cur.execute("DROP TABLE IF EXISTS {t}".format(t=chains_table))
statement = build_create_table_statement('{t}'.format(t=chains_table), chains_cols_types)
cur.execute(statement)
cur.execute("DROP TABLE IF EXISTS {t}".format(t=tracker_table))
statement = build_create_table_statement('{t}'.format(t=tracker_table), tracker_cols_types)
cur.execute(statement)


# Data
G = build_graph(file_path)
Gnodes, Gedges = list(G.nodes()), G.edges()
terminal_nodes = get_terminal_nodes(G)
# Encode nodes
nodes_encoder = objects_encoder(Gnodes)
nodes_decoder = build_decoder(nodes_encoder)
np.save('nodes_encoder.npy', nodes_encoder)
np.save('nodes_decoder.npy', nodes_decoder)
G = nx.relabel_nodes(G, nodes_encoder)
graph_path = os.path.join(sub_graphs_path, 'graph.edgelist')
nx.write_edgelist(G, graph_path)
Gnodes, Gedges = list(G.nodes()), G.edges()
root_node = list(nx.topological_sort(G))[0]
root_successors = list(G.successors(root_node))
for s in root_successors:
    print(s, list(G.successors(s)))
isolates = graph_isolates(G)
Gnodes, Gedges = list(G.nodes()), list(G.edges())
print('Graph with {n} nodes and {e} edges'.format(n=len(Gnodes), e=len(Gedges)))
terminal_nodes = get_terminal_nodes(G)
with open('terminal_nodes.txt', 'w') as f: f.write('\n'.join(terminal_nodes))

saturation_successors, saturation_predecessors = {}, {}
for Gnode in Gnodes:
	if Gnode not in isolates:
		node_successors, node_predecessors = list(G.successors(Gnode)), list(G.predecessors(Gnode))
		successorsDB.set(Gnode, ','.join(node_successors))
		predecessorsDB.set(Gnode, ','.join(node_predecessors))
		saturation_successors[Gnode] = node_successors
		saturation_predecessors[Gnode] = node_predecessors

# Results tables
cur.execute("DROP TABLE IF EXISTS {t}".format(t=tracker_table))
statement = build_create_table_statement('{t}'.format(t=tracker_table), tracker_cols_types)
cur.execute(statement)

# Partitions
# Sub graphs of the source program graph
run_paths = ''
chains = []
for index, root_successor in enumerate(root_successors):
    nodes_to_drop = [root_node] + [s for s in root_successors if s != root_successor] + isolates
    subGnodes = [n for n in Gnodes if n not in nodes_to_drop]
    subG = G.subgraph(subGnodes)
    is_dag = nx.is_directed_acyclic_graph(subG)
    if is_dag:
        #subG = nx.DiGraph(subG)
        print(50*'#')
        print(root_successor, subG)
        root_node = list(nx.topological_sort(subG))[0]
        roots = G.successors(root_node)
        no_preds = [n for n in subG.nodes() if G.predecessors(n) == 0]
        subG_chains = run_pipeline(index, subG, root_successor)
        chains += subG_chains
        print(root_successor, root_node, subG, '{n} chains produced'.format(n=len(subG_chains)))
        # graph_path = os.path.join(sub_graphs_path, 'sub_graph_{i}.edgelist'.format(i=index+1))
        # nx.write_edgelist(subG, graph_path)
        # run_paths += "python3 pipeline.py {gp} & ".format(gp=graph_path)
        # run_path = "python3 pipeline.py {gp} ".format(gp=graph_path)
        # subprocess.run(run_paths, shell=True)
    else:
        print('graph {i} is not dag'.format(i=index+1))
#run_paths = run_paths.rstrip(' &')
#subprocess.run(run_paths, shell=True)
with open('chains.txt', 'w') as f: f.write('\n'.join(chains))










