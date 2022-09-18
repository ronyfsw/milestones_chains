import networkx as nx
file_path = '/home/rony/services/milestones_chains/data/EMS_DCMA_DD_23.08.graphml'
G = nx.read_graphml(file_path)
G = nx.DiGraph(G)
print(G.nodes())
print(G.edges())
