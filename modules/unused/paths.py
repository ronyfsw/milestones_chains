from concurrent.futures import ProcessPoolExecutor
from joblib import Parallel, delayed
import networkx as nx
from itertools import combinations

def list_neighbours(G, node_ids):
	'''
	Identify connected nodes (neighbours) in an input graph
	:param G: Graph objects
	:param node_ids: A list of the nodes to check for connections
	:return: A list of connected nodes
	'''
	neighbours = []
	node_pairs = list(set(combinations(node_ids, 2)))
	for node_pair in node_pairs:
		if G.has_edge(node_pair[0], node_pair[1]):
			neighbours.append(node_pair)
	return neighbours

def list_connected_nodes(G, node_ids):

	'''
	Identify nodes that are not neighbours but are connected via a path in an input graph
	:param G: Graph objects
	:param node_ids: A list of the nodes to check for connections
	:return: A dicrionary holding intermediate nodes (value, list) for each connected node pair (key) in G
	'''

	connected_nodes = []
	node_pairs = list(set(combinations(node_ids, 2)))
	for index, node_pair in enumerate(node_pairs):
		# if index % 10000 == 0: print(index)
		node1, node2 = node_pair[0], node_pair[1]
		if ((not G.has_edge(node1, node2)) & (nx.has_path(G, node1, node2))):
			connected_nodes += list(node_pair)
	connected_nodes = list(set(connected_nodes))
	return connected_nodes

def list_shortest_paths(G, node_ids, num_pairs=None):

	'''
	Identify the shortest paths between nodes that are not neighbours in an input graph
	:param G: Graph objects
	:param node_ids: A list of connected and none-neighbours nodes to check for connections
	:param num_pairs: The number of pairs to use in calculation. If not None the calculation will be applied to the first N pairs only
	:return: A dicrionary holding intermediate nodes (value, list) for each connected node pair (key) in G
	'''
	connected_nodes = {}
	node_pairs = list(set(combinations(node_ids, 2)))
	if num_pairs: node_pairs = node_pairs[:num_pairs]
	print('{n} node pairs'.format(n=len(node_pairs)))
	for index, node_pair in enumerate(node_pairs):
		#if index % 1000 == 0: print(index)
		node1, node2 = node_pair[0], node_pair[1]
		if ((not G.has_edge(node1, node2)) & (nx.has_path(G, node1, node2))):
			nodes_shortest_path = nx.shortest_path(G, source=node1, target=node2)
			if nodes_shortest_path:
				connected_nodes[node_pair] = nodes_shortest_path
				with open('milestone_pairs.txt', 'a') as f:
					node_pair_str = '{n1},{n2}\n'.format(n1=node1, n2=node2)
					f.write(node_pair_str)
	return connected_nodes

def list_shortest_paths_parallel(G, node_ids, num_pairs=None):

	'''
	Identify the shortest paths between nodes that are not neighbours in an input graph
	:param G: Graph objects
	:param node_ids: A list of connected and none-neighbours nodes to check for connections
	:param num_pairs: The number of pairs to use in calculation. If not None the calculation will be applied to the first N pairs only
	:return: A dicrionary holding intermediate nodes (value, list) for each connected node pair (key) in G
	'''

	global node_pair_shortest_path

	def node_pair_shortest_path(node_pair):
		nodes_shortest_path = []
		node1, node2 = node_pair[0], node_pair[1]
		if ((not G.has_edge(node1, node2)) & (nx.has_path(G, node1, node2))):
			nodes_shortest_path = nx.shortest_path(G, source=node1, target=node2)
		return node_pair, nodes_shortest_path

	node_pairs = list(set(combinations(node_ids, 2)))
	if num_pairs: node_pairs = node_pairs[:num_pairs]
	print('{n} node pairs'.format(n=len(node_pairs)))
	connected_nodes = {}
	executor = ProcessPoolExecutor(4)
	for node_pair, nodes_shortest_path in executor.map(node_pair_shortest_path, node_pairs):
		if nodes_shortest_path: connected_nodes[node_pair] = nodes_shortest_path

	return connected_nodes

def has_cycle(G):
    try:
        cycle = list(nx.find_cycle(G, orientation="original"))
        res = True
        # return f"there is at least one cycle in the program \n Cycle:{cycle}", 400
    except nx.NetworkXNoCycle as cyc_ex:
        #print(cyc_ex)
        cycle = []
        res = False
        # raise RuntimeWarning('There are no cycles detected in the graph')
    return res, cycle


