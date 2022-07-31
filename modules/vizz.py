import matplotlib.pyplot as plt
import networkx as nx

# Visualize
def draw_graph(G, file_path):
	'''
	Extension to networkx/matplotlib graph drawing function, adding node names and visual properties
	:param G: Graph object
	:show: A graph plot that can be saved
	'''
	pos = nx.spring_layout(G)
	nx.draw_networkx_nodes(G, pos, cmap=plt.get_cmap('jet'), node_size=500)
	nx.draw_networkx_labels(G, pos)
	nx.draw_networkx_edges(G, pos, edge_color='r')
	nx.draw_networkx_edges(G, pos)
	plt.savefig(file_path)

def draw_graph(G, file_path, node_to_color = None):
	fig, ax = plt.subplots(figsize=(15, 15))
	color_map = []
	if node_to_color:
		for node in G:
			if node == node_to_color:
				color_map.append('red')
			else:
				color_map.append('green')
	nx.draw(G, with_labels=True, node_color=color_map, node_size=100)
	plt.savefig(file_path)


