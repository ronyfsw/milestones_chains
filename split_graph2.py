from modules.config import *
from modules.graphs import *
from modules.vizz import *
from pyvis.network import Network
nt = Network('100%', '100%')
nt.set_options('''var options = {"nodes": {"size": 20, "shape": "triangle", "width":15,
    "font.size":"2"}, "edges":{"width":1, "font.size":"0"}}''')

# file_path = '/home/rony/Projects_Code/Milestones_Chains/tests/data/directed_branched_graph.edgelist'
# G = nx.read_edgelist(file_path)
# G = nx.DiGraph(G)
#root_successors_edges = [('N1', 'N2'), ('N2', 'N1'), ('N1', 'N7'), ('N7', 'N1')]
#nt.from_nx(G)
#t.show('./data/directed_branched_graph.html')
# draw_graph(G, './data/directed_branched_graph.png')
file_path = os.path.join(data_path, data_file_name)
G = build_graph(file_path)
root_node = list(nx.topological_sort(G))[0]
root_successors = tuple(G.successors(root_node))
Gnodes, Gedges = list(G.nodes()), list(G.edges())

edgelist = [e for e in Gedges if e not in root_successors_edges]
G1 = nx.from_edgelist(edgelist, create_using=nx.DiGraph())
Gnodes, Gedges = list(G1.nodes()), list(G1.edges())
draw_graph(G1, './data/broken_graph.png')
a = 0
# for edge in root_successors_edges:
#     #edge = [edge]
#     #Gs = G.remove_edges_from(root_successors_edges)
#     G.remove_edge(*edge)
#
# Gnodes, Gedges = list(G.nodes()), list(G.edges())
# draw_graph(G, './data/directed_branched_graph.png')
# = [G.subgraph(c).copy() for c in nx.connected_components(Gs)]
# subGs = list(nx.weakly_connected_components(Gs))

root_successors_edges = []
for Gedge in Gedges:
    if Gedge[0] == 'MWH.06.M1000':
        root_successors_edges.append(Gedge)
# for Gedge in root_successors_edges:
#     G.remove_edge(*Gedge)
G.remove_edges_from(root_successors_edges)
UG = G.to_undirected()
subGs = [UG.subgraph(c).copy() for c in nx.connected_components(UG)]
for subG in subGs:
    subGnodes, subGedges = list(subG.nodes()), list(subG.edges())
    print(subG)
    # if len(subG) == 1:
    #     print(subGnodes, subGedges)
    # elif len(subG) > 1:
    #     root_node = list(nx.topological_sort(subG))[0]
    #     root_successors = tuple(G.successors(root_node))
    #     b = 0


