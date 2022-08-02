import networkx as nx
G = nx.path_graph(4)  # or DiGraph, MultiGraph, MultiDiGraph, etc
print(G.nodes(), G.edges())
ebunch = [(1, 2), (2, 3)]
G.remove_edges_from(ebunch)
print(G.nodes(), G.edges())
