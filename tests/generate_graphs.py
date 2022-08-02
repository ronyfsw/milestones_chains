import os
import numpy as np
from itertools import combinations
import networkx as nx
from pyvis.network import Network
nt = Network('100%', '100%')
nt.set_options('''var options = {"nodes": {"size": 20, "shape": "triangle", "width":15,
    "font.size":"2"}, "edges":{"width":1, "font.size":"0"}}''')
import sys
modules_path = '/home/rony/Projects_Code/Milestones_Duration/modules'
if modules_path not in sys.path: sys.path.append(modules_path)
from vizz import draw_graph

# # Fully connected graph
# G = nx.Graph()
# node_ids = np.arange(10)
# node_pairs = list(set(combinations(node_ids, 2)))
# G.add_edges_from(node_pairs)
# nx.write_adjlist(G, "./data/fully_connected_graph.adjlist")
# # draw_graph(G)
#
# # list_connected_nodes test graph
# G = nx.Graph()
# G.add_edge('a', 'b', weight=1)
# G.add_edge('a', 'c', weight=1)
# G.add_edge('c', 'd', weight=1)
# G.add_edge('c', 'e', weight=1)
# G.add_edge('c', 'f', weight=1)
# G.add_edge('f', 'g', weight=1)
# G.add_edge('m', 'n', weight=1)
# nx.write_adjlist(G, "./data/list_connected_nodes.adjlist")
# draw_graph(G)

## Branched directed graph
# G = nx.DiGraph()
# G_edges = [('N1', 'N2'), ('N2', 'N3'), ('N2', 'N4'), ('N2', 'N5'), ('N5', 'N6'),\
#            ('N1', 'N7'), ('N7', 'N8'), ('N8', 'N9'), ('N9', 'N10'), ('N10', 'N11'),\
#            ('N7', 'N12'), ('N12', 'N13'), ('N13', 'N14'), ('N13', 'N15'),\
#            ('N15', 'N16'), ('N15', 'N17')]
# G.add_edges_from(G_edges)
# root = list(nx.topological_sort(G))[0]
# print('root:', root)
# nx.write_edgelist(G, "./data/directed_branched_graph.edgelist")
# nt.from_nx(G)
# nt.show('./data/directed_branched_graph.html')

# G = nx.read_edgelist("directed_branched_graph.edgelist", create_using=nx.DiGraph())

# # Small branched directed graph
# G = nx.DiGraph()
# G_edges = [('N1', 'N2'), ('N2', 'N3'), ('N2', 'N4'), ('N2', 'N5'), ('N5', 'N6'),\
#            ('N1', 'N7'), ('N7', 'N8'), ('N8', 'N9')]
# G.add_edges_from(G_edges)
# root = list(nx.topological_sort(G))[0]
# print('root:', root)
# nx.write_edgelist(G, "./data/small_directed_branched_graph.edgelist")
# nt.from_nx(G)
# nt.show('./data/small_directed_branched_graph.html')

# ## Neighbors graph
# G = nx.DiGraph()
# G_edges = [('N7', 'N8'), ('N7', 'N12'), ('N7', 'N1')]
# G.add_edges_from(G_edges)
# nx.write_edgelist(G, "./results/neighbors_graph.edgelist")
# nt.from_nx(G)
# nt.show('./results/neighbors_graph.html')

# Worm walk graph
G_edges = [('N1', 'N2'), ('N2', 'N3'), ('N2', 'N4'), ('N2', 'N5'), ('N3', 'N6'),\
           ('N3', 'N7'), ('N4', 'N8'), ('N4', 'N9')]

# Shortest paths graph
def build_draw_graph(edges_list, file_name):
    G = nx.Graph()
    G.add_edges_from(edges_list)
    # root = list(nx.topological_sort(G))[0]
    # print('root:', root)
    nx.write_edgelist(G, "./data/{fn}.edgelist".format(fn=file_name))
    file_path = os.path.join('./data', '{fn}.html'.format(fn=file_name))
    #draw_graph(G, file_path, node_to_color=root)
    nt.from_nx(G)
    nt.show(file_path)
file_name = 'shortest_paths_graph'
edges_list = [('N1', 'N2'), ('N2', 'N3'), ('N2', 'N4'), ('N2', 'N5'),
           ('N1', 'N7'), ('N2', 'N7'),
           ('N1', 'N9'), ('N9', 'N10'), ('N9', 'N8'), ('N10', 'N3')]
build_draw_graph(edges_list, file_name)