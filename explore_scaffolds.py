import os
import numpy as np
run_dir_path = os.path.join(os.getcwd(), 'run_dir')
scaffolds_path1 = os.path.join(run_dir_path, 'scaffolds')
scaffolds_files = os.listdir(scaffolds_path1)
scaffolds_count = len(scaffolds_files)
nodes_decoder = np.load(os.path.join(run_dir_path, 'nodes_decoder.npy'), allow_pickle=True)[()]

scaffolds_sizes = []
for scaffolds_file in scaffolds_files:
    scaffold_path = os.path.join(scaffolds_path, scaffolds_file)
    scaffold = np.load(scaffold_path, allow_pickle=True)[()]
    print(30*'*')
    print('file: {f} | {n} chains'.format(f=scaffolds_file, n=len(scaffold)))
    for k, chain in scaffold.items():
        encoded_tasks = chain.split(node_delimiter)
        tasks = [nodes_decoder[t] for t in encoded_tasks]
        print('chain {k}| {n} tasks:'.format(k=k, n=len(tasks)), tasks)