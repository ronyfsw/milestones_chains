import os
import pandas as pd
chains_dir = '/home/rony/services/milestones_chains/runs_chains'
files = os.listdir(chains_dir)
file_chains = {}
for file in files:
    file_path = os.path.join(chains_dir, file)
    chains = list(pd.read_parquet(file_path)['Chain'])
    file_chains[file] = chains
for file1, chains1 in file_chains.items():
    for file2, chains2 in file_chains.items():
        if file1 != file2:
            print(30 * '*')
            print('difference between', file1, file2, len(chains1), len(chains2), len(set(chains1)), len(set(chains2)))
            diff1 = set(chains1).difference(set(chains2))
            print(len(diff1), diff1)
            diff2 = set(chains2).difference(set(chains1))
            print(len(diff2), diff2)