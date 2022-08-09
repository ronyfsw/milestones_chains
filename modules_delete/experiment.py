import os
import shutil
from modules.config import *
if experiment_dir in os.listdir(results_path):
    shutil.rmtree(experiment_path, ignore_errors=True)
    os.mkdir(experiment_path)
else:
    os.mkdir(experiment_path)
val_dirs = ['chains', 'milestone_chains']
for dir in val_dirs:
    if dir not in os.listdir(results_path): os.mkdir(os.path.join(results_path, dir))
