import time

with open('terminal_nodes.txt', 'w') as f: f.write('tmp')
from modules.config import *
from modules.libraries import *
from modules.graphs import *
# from modules.chains import *
from modules.encoders import *
from modules.nodes import *
from modules.filters import *
from modules.worm_modules import *
import warnings
warnings.filterwarnings("ignore")

G = build_graph(file_path)
Gnodes, Gedges = list(G.nodes()), G.edges()
# Encode nodes
nodes_encoder = objects_encoder(Gnodes)
nodes_decoder = build_decoder(nodes_encoder)
np.save('nodes_encoder.npy', nodes_encoder)
np.save('nodes_decoder.npy', nodes_decoder)
G = nx.relabel_nodes(G, nodes_encoder)
Gnodes, Gedges = list(G.nodes()), G.edges()
print('Graph with {n} nodes and {e} edges'.format(n=len(Gnodes), e=len(Gedges)))
isolates = graph_isolates(G)
saturation_successors, saturation_predecessors = {}, {}
for Gnode in Gnodes:
	if Gnode not in isolates:
		node_successors, node_predecessors = list(G.successors(Gnode)), list(G.predecessors(Gnode))
		successorsDB.set(Gnode, ','.join(node_successors))
		predecessorsDB.set(Gnode, ','.join(node_predecessors))
		saturation_successors[Gnode] = node_successors
		saturation_predecessors[Gnode] = node_predecessors
terminal_nodes = get_terminal_nodes(G)

# todo: remove following validation
scaffolds_count = redisClient.hlen('scaffolds')
print('{n} scaffolds'.format(n=scaffolds_count))

# Update node saturation
ids = list(redisClient.hkeys('scaffolds'))
scaffolds = list(redisClient.hgetall('scaffolds').values())
scaffolds = [scaffold.split(node_delimiter) for scaffold in scaffolds]
scaffolds = [scaffold[1:] for scaffold in scaffolds if len(scaffold) > 2]
ids_scaffolds = list(zip(ids, scaffolds))

while scaffolds_count >= 1000:
	filter_cids = []
	for cid, scaffold in ids_scaffolds:
		saturation = 0
		# Update node saturation
		for index, node in enumerate(scaffold):
			if index == 0:
				node1 = node
				node_successor = scaffold[index + 1]
				a1 = saturation_successors[node]
				saturation_successors[node] = [n for n in saturation_successors[node] if n != node_successor]
				a2 = saturation_successors[node]
				a = 0
			else:
				node2 = node
				node_predecessor = scaffold[index - 1]
				b1 = saturation_predecessors[node]
				saturation_predecessors[node] = [n for n in saturation_predecessors[node] if n != node_predecessor]
				b2 = saturation_predecessors[node]
				b = 0
			node_saturation = len(saturation_successors[node]) + len(saturation_predecessors[node])
			saturation += node_saturation
			d=0
		#print(cid, saturation)
		if saturation == 0: filter_cids.append(cid)

	print('{n} filter_cids'.format(n=len(filter_cids)))
	for cid in filter_cids:
		redisClient.hdel('scaffolds', cid)
	scaffolds_count = redisClient.hlen('scaffolds')
	print('{n} filtered scaffolds'.format(n=scaffolds_count))

	saturation_successors_count = sum([len(i) for i in list(saturation_successors.values())])
	saturation_predecessors_count = sum([len(i) for i in list(saturation_predecessors.values())])
	cc = 0