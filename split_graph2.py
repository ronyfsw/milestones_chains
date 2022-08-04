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
#root_edges = [('N1', 'N2'), ('N2', 'N1'), ('N1', 'N7'), ('N7', 'N1')]
#nt.from_nx(G)
#t.show('./data/directed_branched_graph.html')
# draw_graph(G, './data/directed_branched_graph.png')
file_path = os.path.join(data_path, data_file_name)
G = build_graph(file_path)
root_node = list(nx.topological_sort(G))[0]
root_successors = list(G.successors(root_node))
Gnodes, Gedges = list(G.nodes()), list(G.edges())

sub_graphs = []
for root_successor in root_successors:
    subGnodes = [n for n in Gnodes if n not in (root_node, root_successor)]
    subG = G.subgraph(subGnodes)
    subG = nx.DiGraph(subG)
    sub_graphs.append(subG)
    a = 0


