import sys
import networkx as nx

modules_path = '/home/rony/Projects_Code/Milestones_Duration/modules'
if modules_path not in sys.path: sys.path.append(modules_path)
from milestones import *



def test_root_chains():
	data = nx.read_edgelist("./data/directed_branched_graph.edgelist", create_using=nx.DiGraph())
	expected_result = [['N1', 'N2'], ['N1', 'N7'], ['N1', 'N2', 'N3'], ['N1', 'N2', 'N4'], \
	                   ['N1', 'N2', 'N5'], ['N1', 'N7', 'N8'], ['N1', 'N7', 'N12'], ['N1', 'N2', 'N5', 'N6'], \
	                   ['N1', 'N7', 'N8', 'N9'], ['N1', 'N7', 'N12', 'N13'], ['N1', 'N7', 'N8', 'N9', 'N10'], \
	                   ['N1', 'N7', 'N12', 'N13', 'N14'], ['N1', 'N7', 'N12', 'N13', 'N15'],
	                   ['N1', 'N7', 'N8', 'N9', 'N10', 'N11'], \
	                   ['N1', 'N7', 'N12', 'N13', 'N15', 'N16'], ['N1', 'N7', 'N12', 'N13', 'N15', 'N17']]
	test_result = root_chains(data)
	with open('./results/chains.txt', 'w') as f:
		for r in expected_result: f.write('{r}\n'.format(r=', '.join(r)))
	assert test_result == expected_result

ids_types = {'A': 'TT_Mile', 'B': 'TT_FinMile', 'C': 'TT_Task', 'D': 'TT_Task', 'E': 'TT_Mile'}
def test_is_milestones_chain():
	data1, data2 = ['A', 'C', 'D', 'C'], ['A', 'C', 'D', 'B']
	test_result1 = is_milestones_chain(data1, ids_types)
	test_result2 = is_milestones_chain(data2, ids_types)
	assert ((not test_result1) & test_result2)

def test_milestone_chains():
	data = [['A', 'C', 'D', 'C'], ['A', 'C', 'D', 'B']]
	expected_result = [['A', 'C', 'D', 'B']]
	test_result = milestone_chains(data, ids_types)
	assert test_result == expected_result
