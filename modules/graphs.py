from modules.libraries import *
# from modules.splitgraph import *
# from modules.paths import *
# from modules.vizz import *

def build_graph(file_path):
	# Graph
	G = nx.read_graphml(file_path)
	G = nx.DiGraph(G)
	return G

def graphs_nodes(graphs):
	nodes = []
	for graph in graphs: nodes += list(graph.nodes())
	return list(set(nodes))

def graph_isolates(G):
	nodes_degrees = dict(G.degree())
	return [n for n in list(nodes_degrees.keys()) if nodes_degrees[n] == 0]

def graph_terminals(G):
	nodes_degrees = dict(G.degree())
	return [n for n in list(nodes_degrees.keys()) if nodes_degrees[n] == 1]

def build_terminals_graphs_dict(indexed_graphs):
	nodes = graphs_nodes(list(indexed_graphs.values()))
	terminals_graphs = {}
	for node in nodes:
		terminals_graphs[node] = []
		for index, graph in indexed_graphs.items():
			if node in graph_terminals(graph):
				terminals_graphs[node].append(index)
	terminals_graphs = {k: v for k, v in terminals_graphs.items() if len(v)>1}
	return terminals_graphs

def get_graph_terminals_to_crop(terminal, indices_graphs):
	# Identify the largest graph containing the terminal
	graphs_sizes = {index: len(graph.nodes()) for index, graph in indices_graphs.items()}
	min_size = min(graphs_sizes.values())
	min_sized_graph_index = [index for index, size in graphs_sizes.items() if size == min_size][0]
	# Crop the terminal from all graphs which are larger than min_size
	graphs_to_crop = {index: graph for index, graph in indices_graphs.items() if index != min_sized_graph_index}
	graphs_terminals = []
	for index, graph in graphs_to_crop.items():
		graphs_terminals.append((index, terminal))
		a = 0
	return graphs_terminals

def crop_terminals(indexed_graphs):
	start = time.time()
	terminals_graphs = build_terminals_graphs_dict(indexed_graphs)
	print('terminal graphs duration, step 0=', round(time.time() - start))
	step = 1
	while len(terminals_graphs) > 0:
		graph_terminals_collect = []
		start = time.time()
		for terminal, indices in terminals_graphs.items():
			indices_graphs = {k: v for k, v in indexed_graphs.items() if k in indices}
			graph_terminals_to_crop = get_graph_terminals_to_crop(terminal, indices_graphs)
			graph_terminals_collect += graph_terminals_to_crop
		a = 0
		# Graph tuples to dictionary
		graph_tuples_dict = {}
		for graph_tuple in graph_terminals_collect:
			graph_index, terminal = graph_tuple
			if graph_index not in graph_tuples_dict.keys():
				graph_tuples_dict[graph_index] = [terminal]
			else:
				graph_tuples_dict[graph_index].append(terminal)

		# Remove terminals of graphs
		for graph_index, terminals in graph_tuples_dict.items():
			graph = indexed_graphs[graph_index]
			# graph.remove_nodes_from(terminals)
			for terminal in terminals: graph.remove_node(terminal)
			indexed_graphs[graph_index] = graph
			a = 0
		start = time.time()
		terminals_graphs = build_terminals_graphs_dict(indexed_graphs)
		print('terminal graphs duration, step {s}='.format(s=step), round(time.time() - start))
		step += 1
	return indexed_graphs


def predecessors_successors(file_path):
	'''Edges direction validation table
	example: <edge id="MWH06-10609-MWH06-10608" source="MWH06-10609" target="MWH06-10608"> -> nodes as source and target
	'''
	graphml_edges = [l for l in open(file_path).read().split('\n') if 'edge id' in l]
	pairs = [tuple(re.findall('[source|target]="(.+?)"', l)) for l in graphml_edges]
	edge_relations = pd.DataFrame(pairs, columns=['predecessor', 'successor'])
	#predecessors, successors = list(edge_relations['predecessor']), list(edge_relations['successor'])
	return edge_relations

def partition_graph(G, file_path, partition_size_cutoff):
	Gnodes = list(G.nodes())
	edge_relations = predecessors_successors(file_path)
	predecessors, successors = list(edge_relations['predecessor']), list(edge_relations['successor'])
	source_isolates = graph_isolates(G)
	partitions,  connected_nodes, tracker = {}, [], []
	seeds = [n for n in Gnodes if n not in source_isolates]
	partition_index = 0

	# Build partitions
	for index, seed in enumerate(seeds):
		partition_edges = get_successors_edges(seed, G)
		NG = nx.from_edgelist(partition_edges)
		NGdegrees = dict(NG.degree())
		outer_nodes = [n for n in NGdegrees.keys() if (NGdegrees[n] == 1)]
		for outer_node in outer_nodes:
			outer_node_neighbors_edges = get_successors_edges(outer_node, G)
			# Validate partition edges
			for edge_pair in outer_node_neighbors_edges:
				p1, p2 = edge_pair
				if p1 in predecessors:
					predecessor = edge_relations['successor'][edge_relations['predecessor']==p1].values[0]
					if p2 == predecessor:
						partition_edges.append(edge_pair)
			partition_edges += outer_node_neighbors_edges
			if len(partition_edges) < partition_size_cutoff:
				NG = nx.from_edgelist(partition_edges, create_using=nx.DiGraph)
		if NG:
			partition_index += 1
			partitions[partition_index] = NG
		partition_has_cycle, cycle = has_cycle(NG)
		connected_nodes = list(set(connected_nodes + [seed] + list(NG.nodes())))
		a = 0
		nodes_degrees = dict(NG.degree())
		isolates_count = len([n for n in list(nodes_degrees.keys()) if nodes_degrees[n] == 0])
		tracker_row = [index, seed, len(set(NG.nodes())), len(set(NG.edges())), len(partition_edges),\
		            isolates_count, str(partition_has_cycle)]
		tracker.append(tracker_row)
	partitioning_tracker = pd.DataFrame(tracker, columns=['step', 'seed', 'nodes', 'edges', 'partition_edges',\
	                                            'isolates', 'has cycle'])
	return partitions, partitioning_tracker

