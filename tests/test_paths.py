import networkx as nx
import numpy as np
import sys
modules_path = '/home/rony/Projects_Code/Milestones_Duration/modules'
if modules_path not in sys.path: sys.path.append(modules_path)
from paths import *
from vizz import *
import collections

# def test_list_neighbours():
# 	data = nx.read_adjlist("./data/list_connected_nodes.adjlist")
# 	expected_result = [('a', 'c'), ('c', 'd'), ('c', 'f')]
# 	node_ids = ['a', 'c', 'd', 'f']
# 	test_result = list_neighbours(data, node_ids)
# 	assert collections.Counter(test_result) == collections.Counter(expected_result)

# def test_list_shortest_paths():
# 	data = nx.read_adjlist("./data/list_connected_nodes.adjlist")
# 	node_ids = ['a', 'c', 'f', 'd', 'g']
# 	test_result = list_shortest_paths(data, node_ids)
# 	#np.save('./results/list_shortest_paths.npy', test_result)
# 	print('\ntest_result:')
# 	for k, v in test_result.items(): print('{k}:{v}'.format(k=k, v=v))
# 	expected_result = np.load('./results/list_shortest_paths.npy', allow_pickle=True)[()]
# 	assert test_result == expected_result



def test_list_connected_nodes():
	data = nx.read_adjlist("./data/list_connected_nodes.adjlist")
	node_ids = ['a', 'c', 'f', 'd', 'g', 'm', 'n']
	test_result = list_connected_nodes(data, node_ids)
	expected_result = ['c', 'g', 'a', 'f', 'd']
	assert collections.Counter(test_result) == collections.Counter(expected_result)

