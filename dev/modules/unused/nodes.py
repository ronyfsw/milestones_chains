import networkx as nx
import pandas as pd

def count_node_types(G, type_key = 'TaskType'):
	'''
	Count the types of nodes in an input graph
	:param G: Graph object
	:param type_key: The key in the node attributes indexing the node type
	:return: Graph node types and their counts arranged in a table (dataframe)
	'''
	G_nodes = G.nodes()
	node_types = []
	for node in G_nodes: node_types.append(G_nodes[node][type_key])
	types_count = pd.Series(node_types).value_counts()
	types_count = pd.DataFrame(list(zip(list(types_count.index), list(types_count.values))),\
	                           columns=['Type', 'Count'])
	return types_count


def map_junction(cid, growth_tip_successors):
	'''
	Map the junction around the last node in a chain . A junction consists of the node and its successors
	:param tip (str): The node to explore
	:param cid (int): The ID of the chain of nodes leading to the node
	:param growth_tip_successors:
	:return (tuple): The tip successors (tuple) indexed by the chain id
	'''
	junction_map = ()
	if growth_tip_successors:
		growth_tip_successors = tuple(growth_tip_successors.split(','))
		junction_map = (cid, growth_tip_successors)
	return junction_map

