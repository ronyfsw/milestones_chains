from pathlib import Path
import os
import sys
home_dir = Path.home()
modules_dir = os.path.join(home_dir, 'services/milestones_chains/modules/')
if modules_dir not in sys.path: sys.path.append(modules_dir)
from config import *

# Directories
if 'run_dir' in os.listdir(working_dir):
	shutil.rmtree(run_dir_path)
os.mkdir(run_dir_path)
run_dirs = os.listdir(run_dir_path)
if 'chunks' not in run_dirs:
	os.mkdir(chunks_path)
if 'scaffolds' not in run_dirs:
	os.mkdir(scaffolds_path)
if 'sub_graphs' not in run_dirs:
	os.mkdir(sub_graphs_path)