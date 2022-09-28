import numpy as np
import os
run_dir_path = os.path.join(os.getcwd(), 'run_dir')
scaffolds_path = os.path.join(run_dir_path, 'scaffolds')
scaffolds = os.listdir(scaffolds_path)
scaffolds_count = []
for scaffold in scaffolds:
    scaffold_path = os.path.join(scaffolds_path, scaffold)
    scaffold_dict = np.load(scaffold_path, allow_pickle=True)[()]
    scaffolds_count.append(len(scaffold_dict))
scaffolds_count.sort()
print(len(scaffolds_count), scaffolds_count)
