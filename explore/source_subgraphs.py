from modules.config import *
from modules.graphs import *
data_file_path = '/home/rony/services/milestones_chains/data/EMS_DCMA_DD_23.08.graphml'
G = build_graph(data_file_path)
root_node = list(nx.topological_sort(G))[0]
# Identify sub_graphs
sub_graphs = [sg for sg in list(nx.connected_components(G.to_undirected())) if len(sg)>1]
c1, c2, sub_graphs_nodes = [], [], []
for sub_graph in sub_graphs:
    if root_node in sub_graph: c1.append(len(sub_graph))
    else:
        c2.append(len(sub_graph))
        sub_graphs_nodes += list(sub_graph)
print('The graph contains {n1} subgraphs connected to the root and {n2} subgraphs which are disconnected from the root'
      .format(n1=len(c1), n2=len(c2)))
sub_graphs_nodes = list(set(sub_graphs_nodes))
print('{n} tasks will be excluded from the analysis as they reside in the disconnected sub graphs:'.format(n=len(sub_graphs_nodes)), sub_graphs_nodes)
