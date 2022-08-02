import sys
import networkx as nx
modules_path = '/home/rony/Projects_Code/Milestones_Duration/modules'
if modules_path not in sys.path: sys.path.append(modules_path)
from splitgraph import neighbors_graph


print('imported')
def test_neighbors_graph():
	data = nx.read_edgelist("./data/directed_branched_graph.edgelist", create_using=nx.DiGraph())
	test_result = neighbors_graph('N7', data)
	expected_result = nx.read_edgelist("./results/neighbors_graph.edgelist")#, create_using=nx.DiGraph())
	assert test_result == expected_result
