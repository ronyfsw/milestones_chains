from pathlib import Path
import os
import sys
home_dir = Path.home()
modules_dir = os.path.join(home_dir, 'services/milestones_chains/modules/')
if modules_dir not in sys.path: sys.path.append(modules_dir)
from config import *

# Directories
working_dir = os.getcwd()
run_dir_path = os.path.join(working_dir, 'run_dir')
if 'run_dir' in os.listdir(working_dir):
	shutil.rmtree(run_dir_path)
os.mkdir(run_dir_path)
chunks_path = os.path.join(run_dir_path, 'chunks')
sub_graphs_path = os.path.join(run_dir_path, 'sub_graphs')
scaffolds_path = os.path.join(run_dir_path, 'scaffolds')
run_dirs = os.listdir(run_dir_path)
if 'chunks' not in run_dirs:
	os.mkdir(chunks_path)
if 'scaffolds' not in run_dirs:
	os.mkdir(scaffolds_path)
if 'sub_graphs' not in run_dirs:
	os.mkdir(sub_graphs_path)