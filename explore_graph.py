# run statement: python service.py <file_name> <experiment_name> <'tdas'> <'prt'>
import pandas as pd

print('start')
import os, sys, pathlib
modules_dir = os.path.join(pathlib.Path.home(), 'services/milestones_chains/modules/')
if modules_dir not in sys.path: sys.path.append(modules_dir)
#from modules.worm_modules import *

from graphs import *

# parser = argparse.ArgumentParser()
# parser.add_argument('data_file_name')
# args = parser.parse_args()
# data_file_name = args.data_file_name

files = ['MWH-06-UP#13_FSW_REV.graphml', 'MWH06BLGraphML_File_.graphml']

# Data
for data_file_name in files:
    print('file name: {f}'.format(f=data_file_name))
    path = os.path.join('./data', data_file_name)
    G = build_graph(path)
    Gnodes, Gedges = list(G.nodes()), G.edges()
    print('Graph with {n} nodes and {e} edges'.format(n=len(Gnodes), e=len(Gedges)))
    file_size = round(sys.getsizeof(G)/(1024*1024), 6)
    print('Graph size = {s} MB'.format(s=file_size))
    file_size = round(sys.getsizeof(path) / (1024 * 1024), 6)
    print('File size = {s} MB'.format(s=file_size))