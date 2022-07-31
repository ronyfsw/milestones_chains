import os

import pandas as pd

from modules.config import *
from modules.libraries import *
from modules.graphs import *
from modules.chains import *
from modules.encoders import *
from modules.filters import *
from modules.worm_modules import *
start = time.time()
import warnings
warnings.filterwarnings("ignore")

# Data
G = build_graph(file_path)

#file_path = '/home/rony/Projects_Code/Milestones_Duration/tests/data/worm_walk_demo.edgelist'
#G = nx.read_edgelist(file_path, create_using=nx.DiGraph())
Gnodes, Gedges = list(G.nodes()), G.edges()
isolates = graph_isolates(G)
print('Graph with {n} nodes and {e} edges'.format(n=len(Gnodes), e=len(Gedges)))
print(list(G.successors('MWH.06.M4010')))
ns = [n for n in G.nodes() if\
      list(G.successors(n)) ==\
      ['MWH06.C2.CS1010', 'MWH06.C3.CS1010', 'MWH06-C3.P1000', 'MWH-06-02-SUP-S', 'MWH-06-02-DRY-F']]
print(ns)
root_node = list(nx.topological_sort(G))[0]
error_pairs = ['MWH06.AD.CS1060<>MWH06.C1.CS1060', 'MWH06AD-1010<>MWH06.C1.C2100', 'MWH06-1997<>MWH06-C1.P1400', 'MWH06.C4.CS1090<>MWH06.C3.C3190', 'MWH06-10075<>MWH06.S.1100', 'MWH06.C2.C4370<>MWH06-C1.P1100', 'MWH06.C4.C3220<>MWH06.C2.CS1120', 'MWH06.C2.CS1120<>MWH06-1965', 'MWH06.C3.C2290<>MWH06.C3.C2300', 'MWH06-2508<>MWH06-9744', 'MWH06-1997<>MWH06-1977', 'MWH06.C4.C3220<>MWH06.C3.C3030', 'MWH06-2508<>MWH06-2505', 'MWH06.C4.CS1090<>MWH06.C2.CS1120', 'MWH.06.M1010<>MWH-06-01-EPMS-F', 'MWH06-1997<>MWH-06-03-PAD-F', 'MWH06.C2.C4370<>MWH-06-01-EW-F', 'MWH06.C3.Cx4000<>MWH-06-02-SOG-F', 'MWH06.AD.CS1060<>MWH06-1965', 'MWH06-1997<>MWH-06-02-MOB-S', 'MWH06AD-1010<>MWH06.C1.C2160', 'MWH06.C4.CS1090<>MWH06.C3.C2240', 'MWH06-1997<>MWH06-2000', 'MWH06-2508<>MWH-06-01-SOG-F', 'MWH06-C3.P1900<>MWH06-10075', 'MWH06.C3.C2190<>MWH06-10313', 'MWH06.C3.C2190<>MWH06.D6.C3.C2130', 'MWH06-1997<>MWH06.AD.CS1070', 'MWH06.C2.C4370<>MWH06-1998', 'MWH06.C2.C4370<>MWH-06-02-ROJ-F', 'MWH06AD-1010<>MWH06.C1.C1080', 'MWH06AD-1010<>MWH06.C1.C2110', 'MWH06.C3.Cx4000<>MWH06-C2.P1500', 'MWH06AD-1010<>MWH06.C1.C2030', 'MWH06.C3.Cx4000<>MWH06-10075', 'MWH06AD-1010<>MWH06.C1.C4110', 'MWH06.C3.C2190<>MWH06-10357', 'MWH06AD-1017<>MWH06.S.1020', 'MWH06-2508<>MWH06AD-1021', 'MWH06AD-1010<>MWH06.C1.CS1120', 'MWH06-1997<>MWH06-2003', 'MWH06.C3.Cx4000<>MWH-06-04-SOG-F', 'MWH06.C2.C4250<>MWH06-9938', 'MWH06AD-1025<>MWH06.S.1100', 'MWH06.C2.C3230<>MWH-06-02-L1CX-F', 'MWH06.C4.CS1090<>MWH06.C3.C1030']
error_nodes = []
for p in error_pairs: error_nodes = error_nodes + [p.split('<>')[1]]# + [p.split('<>')[1]]
error_nodes = list(set(error_nodes))
for n in Gnodes:
	print(30*'*')
	if n in error_nodes:
		e = 'error'
	else: e = ''
	print('{e}>{n}'.format(e=e, n=n))
	print('predecessors:', G.in_degree(n), list(G.predecessors(n)))
	print('successors:', G.out_degree(n), list(G.successors(n)))
