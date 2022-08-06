import time
import subprocess

with open('terminal_nodes.txt', 'w') as f: f.write('tmp')
from modules.config import *
from modules.libraries import *
from modules.graphs import *
from modules.chains import *
from modules.encoders import *
from modules.nodes import *
from modules.filters import *
from modules.worm_modules import *
import warnings
warnings.filterwarnings("ignore")

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
Gnodes, Gedges = list(G.nodes()), G.edges()

isolates = graph_isolates(G)
saturation_successors, saturation_predecessors = {}, {}
for Gnode in Gnodes:
	if Gnode not in isolates:
		node_successors, node_predecessors = list(G.successors(Gnode)), list(G.predecessors(Gnode))
		successorsDB.set(Gnode, ','.join(node_successors))
		predecessorsDB.set(Gnode, ','.join(node_predecessors))
		saturation_successors[Gnode] = node_successors
		saturation_predecessors[Gnode] = node_predecessors

print('Graph with {n} nodes and {e} edges'.format(n=len(Gnodes), e=len(Gedges)))
isolates = graph_isolates(G)
terminal_nodes = get_terminal_nodes(G)
with open('terminal_nodes.txt', 'w') as f: f.write('\n'.join(terminal_nodes))

# Results tables
cur.execute("DROP TABLE IF EXISTS {t}".format(t=tracker_table))
statement = build_create_table_statement('{t}'.format(t=tracker_table), tracker_cols_types)
cur.execute(statement)

# Partitions
root_node = list(nx.topological_sort(G))[0]
root_successors = list(G.successors(root_node))
Gnodes, Gedges = list(G.nodes()), list(G.edges())

# Sub graphs of the source program graph
sub_graphs = []
for root_successor in root_successors:
    subGnodes = [n for n in Gnodes if n not in (root_node, root_successor)]
    subG = G.subgraph(subGnodes)
    subG = nx.DiGraph(subG)
    sub_graphs.append(subG)

for subG in sub_graphs:
	run_pipeline_args = (subG, terminal_nodes, conn_params)
	pipeline = threading.Thread(target=build_chains, args=run_pipeline_args)
	pipeline.start()
	pipeline.join(0)










