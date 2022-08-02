from itertools import combinations
import pandas as pd
import os
import re
import time
import networkx as nx

def build_chains_terminals_dicts(chains):
	chains_start, chains_end = {}, {}
	for chain in chains:
		chains_start[tuple(chain)] = chain[0]
		chains_end[tuple(chain)] = chain[-1]
	return chains_start, chains_end

def chains_from_linear_graph(G):
	chain = []
	if len(G) == 2:
		chain = list(G.nodes())
	else:
		max_degree = max([d[1] for d in G.degree()])
		if max_degree == 2:
			try:
				chain = list(nx.topological_sort(G))
			except nx.exception.NetworkXError:
				Gdegrees = dict(G.degree())
				outer_nodes = [n for n in Gdegrees.keys() if Gdegrees[n] == 1]
				chain = list(nx.all_simple_paths(G, outer_nodes[0], outer_nodes[1]))[0]
	return chain

def chains_from_linear_graphs(indexed_graphs):
	chains = {}
	# Single-chain graphs
	for index, graph in indexed_graphs.items():
		if len(graph)>0:
			chain = chains_from_linear_graph(graph)
			if chain:
				chains[index] = chain
	return chains

def chains_from_star_graph(G):
	chains = []
	Gdegrees = dict(G.degree())
	outer_nodes = [n for n in Gdegrees.keys() if (Gdegrees[n] == 1)]
	seed = [n for n in Gdegrees.keys() if (Gdegrees[n] > 1)][0]
	outer_node_pairs = list(set(combinations(outer_nodes, 2)))
	for p1, p2 in outer_node_pairs:
		chains.append([p1, seed, p2])
	return chains

def chains_from_star_graphs(indexed_graphs, graph_size_cutoff):
	star_chains = {}
	for index, graph in indexed_graphs.items():
		Gdegrees = dict(graph.degree())
		max_node = [k for k, v in Gdegrees.items() if v > 1]
		# Star signal: Only one node has more than one links
		if len(max_node) == 1:
			chains = chains_from_star_graph(graph)
			if chains:
				star_chains[index] = chains
	return star_chains

def nodes_chains(chains):
	nodes = []
	for chain in chains: nodes += chain
	return nodes

def extend_pair_chains(pair):
	a, b = pair
	extended_chains = []
	interecting_nodes = set(a).intersection(set(b))
	for n in interecting_nodes:
		extension1, extension2 = [], []
		aindex, bindex = (a.index(n), b.index(n))
		# If an intersecting node is a chains' start node
		if aindex == 0:
			a = a[1:]
			extension1 = b + a
		elif bindex == 0:
			b = b[1:]
			extension1 = a + b
		# If an intersecting node is a chains' end node
		elif aindex == len(a) -1:
			a = a[:-1]
			extension1 = a + b
		elif bindex == len(b) -1:
			b = b[:-1]
			extension1 = b + a
		# If an intersecting node is inside a chain
		else:
			extension1 = a[:aindex] + b[bindex:]
			extension2 = b[:bindex] + a[aindex:]
		extensions = [e for e in [extension1, extension2] if e]
		extended_chains += extensions
	return extended_chains

def extend_chunk_pairs(index_path):
	chunk_index, data_path, results_path = index_path
	extended_chains, chunk_exclude, exclude_indices = [], [], []
	pairs_df0 = pd.read_pickle(os.path.join(data_path, 'chunk{c}.pkl'.format(c=chunk_index)))
	pairs = [tuple(p) for p in pairs_df0.values.tolist()]
	for index, pair in enumerate(pairs):
		extended_chains += extend_pair_chains(pair)
		a=0
	extended_chains = list(set(extended_chains))
	if extended_chains:
		extended_chains = '\n'.join([str(c) for c in extended_chains])
		with open(os.path.join(results_path, 'chunk{c}.txt'.format(c=chunk_index)), 'w') as f: f.write(extended_chains)
	return len(extended_chains)

def dict_chains_to_chains(key_chains_dict):
	chains = []
	for node, pchains in key_chains_dict.items(): chains += pchains
	return chains

def chains_nodes_count(chains):
	nodes = []
	for chain in chains: nodes += chain
	return (len(set(nodes)))

def lists_items(items_lists):
	items = []
	for items_list in items_lists:
		items_list = [c[0] if type(c) == list else c for c in items_list]
		items += items_list
	return items

def encode_text_strings(items_lists):
	items = lists_items(items_lists)
	return serial_encoder(items)

def hash_lists(items_lists, items_map, result_type = 'tuple'):
	hashed_lists = []
	for items_list in items_lists:
		items_list = [items_map[n] for n in items_list]
		hashed_lists.append(tuple(items_list))
	return hashed_lists

def hash_chunk_chains(index_path):
	chunk_index, chains_map, pairs_path, hashed_chains_path = index_path
	pairs_df0 = pd.read_pickle(os.path.join(pairs_path, 'chunk{c}.pkl'.format(c=chunk_index)))
	pairs = [tuple(p) for p in pairs_df0.values.tolist()]
	hashed_pairs = []
	for index, pair in enumerate(pairs):
		hashed_pair = []
		for items_list in pair:
			items_list = items_list.split(',')
			hashed_list = str([chains_map[item] for item in items_list])
			hashed_list = re.sub("\[|\]|'|\s", '', hashed_list)
			hashed_pair.append(hashed_list)
			a = 0
		hashed_pairs.append(tuple(hashed_pair))
	hashed_pairs_df = pd.DataFrame(hashed_pairs, columns=['p1', 'p2'])
	hashed_pairs_df.to_pickle(os.path.join(hashed_chains_path, 'chunk{c}.pkl'.format(c=chunk_index)))

	return hashed_pairs_df

def pairs_chunks(items_path, chunk_size = 1000):
	'''
	Build chain pairs combination in chunks to avoid memory issues
	:param items(list): The items to pair
	'''
	start = time.time()
	data_chunk_index, chains_path, results_path = items_path
	items = open(os.path.join(chains_path, 'chunk{i}.txt'.format(i=data_chunk_index))).read().split('\n')
	chunk_index = data_chunk_index
	while items:
		chunk_index = round(chunk_index + 0.1, 4)
		items_chunk = items[:chunk_size]
		items = items[chunk_size:]
		items_chunk_pairs = list(set(combinations(items_chunk, 2)))
		chunk_df = pd.DataFrame(items_chunk_pairs, columns=['p1', 'p2'])
		write_path = os.path.join(results_path, 'chunk{c}.pkl'.format(c=chunk_index))
		chunk_df.to_pickle(write_path)
	print('chunking duration=', time.time() - start)
	return 'finished'

# Root chains and helpers
def extend_chain(tail_successors_submitted):
	'''
	Extend a chain of nodes using the successors of the last node in the chain
	:param successors(list): The chain to extend followed by a dictionary of link types for each combination of the last node in the chain and a successor
	Example:
	['MWH.06.M1000'], {('MWH.06.M1000', 'A1170'): 'FS', ('MWH.06.M1000', 'MWH06-2029'): 'FS', ('MWH.06.M1000', 'MWH06-2527'): 'FS', ('MWH.06.M1000', 'MWH06.D1.S1010'): 'FS'...}
	:return:
	'''
	tail, successors = tail_successors_submitted
	tail_successors = []
	for successor in successors:
		tail_successor = [tail, successor]
		tail_successors.append(tail_successor)
	return tail_successors

# todo uild_edges_types and apply to the complete chains
# def build_edges_types
	# Gedges = G.edges(data=True)
	# edges_types = {}
	# for Gedge in Gedges: edges_types[(Gedge[0], Gedge[1])] = Gedge[2]['Dependency']


def write_rows_bulk(rows, table_name, tmp_path, conn):
	rows = '\n'.join(rows)
	with open(tmp_path, 'w') as f: f.write(rows)
	statement = "LOAD DATA INFILE '{tp}' INTO TABLE {tn} LINES TERMINATED BY '\n'".format(
		tp=tmp_path, tn=table_name)
	c = conn.cursor()
	c.execute(statement)
	conn.commit()


def read_chains(path, how='pickle'):
	if how == 'pickle':
		chains = list(set(pd.read_pickle(path)['chain']))
	elif how == 'text':
		chains = open(path).read().split('\n')
	return chains

def root_chains(G):
	'''
	Identify node chains in a directed graph that start from the root node
	:param G: DiGraph object
	:param Gindex: The Graph object (for validation only)
	:return: List of node chains
	'''
	Gnodes = G.nodes()
	# nodes_hash_map = serial_encoder(Gnodes)
	# hash_nodes_map = {v: k for k, v in nodes_hash_map.items()}
	# G = nx.relabel_nodes(G, nodes_hash_map)
	Gnodes, Gedges = G.nodes(), G.edges()
	nodes_count = len(Gnodes)
	print('{n2} unique edges between {n1} nodes'.format(n1=len(Gnodes), n2=len(set(Gedges))))
	root_node = list(nx.topological_sort(G))[0]
	chains = [[root_node]]
	tmp_path = os.path.join(os.getcwd(), 'chains_temp.pkl')
	write_chains(chains, tmp_path)

	# Load root node
	nodes_visited, nodes_visited_count, milestones_chain_count = [], 0, 0

	step = 0
	while nodes_visited_count != nodes_count:
		# Step params
		start1 = time.time()
		step += 1
		write_chunk, chains_produced_count, write_chunks_count = 10000, 0, 0
		print(50 * '=')
		print('step {s}| {n1} nodes, of which {n2} were visited'\
		      .format(s=step, n1=nodes_count, n2=nodes_visited_count))

		chains_fetched = read_chains(tmp_path, how='pickle')
		chains_fetched = [re.split(',', chain) for chain in chains_fetched]
		for chain in chains_fetched:
			chain = [n.rstrip().lstrip() for n in chain]
			chains.append(chain)
		for chain in chains:
			nodes_visited += chain
		del chains_fetched
		chains_count = len(chains)
		print('Tracking successors for {n} chains'.format(n=chains_count))

		# Collect the successor tasks for the last task in the chain
		tail_successors_submit = []
		tails_chains_dict = {}
		for index, chain in enumerate(chains):
			tail = chain[-1]
			successors = list(G[tail].keys())
			tail_successors_submit.append((tail, successors))
			tails_chains_dict[tuple(chain)] = tail

		# Extend chains using the last node successors of each
		chains_to_write = []
		for tails_chains in map(extend_chain, tail_successors_submit):
			chains_produced_count += len(tails_chains)
			# Concatenate a chain tail successors to it's chain
			for tail_chain in tails_chains:
				tail, chain = tail_chain[0], tail_chain[1:]
				chains_to_extend = [k for k, v in tails_chains_dict.items() if v == tail]
				for chain_to_extend in chains_to_extend:
					extended_chain = list(chain_to_extend) + list(chain)
					chains_to_write.append(extended_chain)

		write_chains(chains_to_write, tmp_path,  how='pickle')

		del chains_to_write #, milestone_chains
		nodes_visited_count = len(set(nodes_visited))
		print('{n} nodes visited'.format(n=nodes_visited_count))
		print('iteration duration=', round(time.time()-start1))

		# Validation results
		# c.execute("SELECT chain FROM milestone_chains;".format(v=root_node))
		# milestone_chains = '\n'.join([i[0] for i in c.fetchall()])
		# with open('./results/validation/milestone_chains/step_{s}.txt'.format(s=step), 'w') as f: f.write(milestone_chains)

		# c.execute("SELECT chain FROM chains;".format(v=root_node))
		# chains_read = [(i[0]) for i in c.fetchall()]
		# chains = []
		# for chain in chains_read:
		# 	chain = '<>'.join([hash_nodes_map[float(t)] for t in chain.split('-')])
		# 	chains.append(chain)
		# chains_str = '\n'.join(chains)
		# with open('./results/validation/chains/graph_{i}_step_{s}.txt'\
		# 		          .format(i=Gindex, s=step), 'w') as f: f.write(chains_str)

	chains = read_chains(tmp_path, how='pickle')
	return chains