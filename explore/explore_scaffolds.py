import os
import numpy as np
import pandas as pd
node_delimiter = '<>'
run_dir_path = os.path.join(os.getcwd(), 'run_dir')
scaffolds_path = os.path.join(run_dir_path, 'scaffolds')
scaffolds_files = os.listdir(scaffolds_path)
scaffolds_count = len(scaffolds_files)
nodes_decoder = np.load(os.path.join(run_dir_path, 'nodes_decoder.npy'), allow_pickle=True)[()]

results = []
for scaffolds_file in scaffolds_files:
    scaffold_path = os.path.join(scaffolds_path, scaffolds_file)
    scaffold = np.load(scaffold_path, allow_pickle=True)[()]
    # print(30*'*')
    # print('file: {f} | {n} chains'.format(f=scaffolds_file, n=len(scaffold)))
    chains_count = len(scaffold)
    tasks_count = 0
    for k, chain in scaffold.items():
        encoded_tasks = chain.split(node_delimiter)
        tasks_count += len(encoded_tasks)
        #tasks = [nodes_decoder[t] for t in encoded_tasks]
        #print('chain {k}| {n} tasks:'.format(k=k, n=len(tasks)), tasks)

    tasks_per_chain = round(tasks_count/chains_count)
    results.append([scaffolds_file, chains_count, tasks_per_chain])
    results_df = pd.DataFrame(results, columns=['scaffolds_file', 'chains_count', 'tasks_per_chain'])
    print(results_df)
    results_df.to_excel('explore_scaffolds.xlsx', index=False)