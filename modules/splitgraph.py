import os
import copy
import networkx as nx
import numpy as np
import pandas as pd
import itertools

def get_successors_edges(node, G):
	#node_predecessors = list(G.predecessors(node))
	node_successors = list(G.successors(node))
	neighbors_edges = []
	for neighbor in node_successors:
		neighbors_edges.append((node, neighbor))
	# for neighbor in node_predecessors:
	# 	neighbors_edges.append((neighbor, node))
	return neighbors_edges

from itertools import combinations


def graph_to_chains(G):
	#nt.from_nx(G)
	#nt.show('sub_graph.html')
	chains = []
	Gdegrees = dict(G.degree())
	outer_nodes = [n for n in Gdegrees.keys() if Gdegrees[n] == 1]
	outer_nodes_combinations = list(itertools.combinations(outer_nodes, 2))
	for nodes_comb in outer_nodes_combinations:
		nodes_comb_chains = list(nx.all_simple_paths(G, nodes_comb[0], nodes_comb[1]))
		for nodes_comb_chain in nodes_comb_chains:
			chains.append(nodes_comb_chain)
	return G, chains

def graph_to_chains(G):
	chains = []
	Gdegrees = dict(G.degree())
	outer_nodes = [n for n in Gdegrees.keys() if (Gdegrees[n] == 1)]
	outer_node_pairs = list(set(combinations(outer_nodes, 2)))
	for p1, p2 in outer_node_pairs:
		pair_chains = list(nx.all_simple_paths(G, p1, p2))
		chains += pair_chains
	return chains