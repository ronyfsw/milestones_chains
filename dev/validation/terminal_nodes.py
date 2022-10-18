import networkx as nx
from modules.graphs import *
from modules.worm_modules import *
file_path = '/data/MWH-06-UP#13_FSW_REV.graphml'
G = build_graph(file_path)
Gnodes, Gedges = list(G.nodes()), G.edges()
terminal_nodes = get_terminal_nodes(G)
node = 'MWH-06-04-UGU-F'
c = node in terminal_nodes
d = list(G.successors(node))
a=0
