import os

import numpy as np

from modules.config import *
run_dir_path = os.path.join(os.getcwd(), 'run_dir')
scaffolds_path1 = os.path.join(run_dir_path, 'scaffolds')
sub_graphs_path1 = os.path.join(run_dir_path, 'sub_graphs')

data_file_name = 'EMS_DCMA_DD_23.08.graphml'
experiment = None
tasks_types = 'milestones'
results = 'no'
if not experiment: experiment = data_file_name.replace('.graphml', '').replace('.', '_')
counts = []

for i in range(30):
    print('run', i+1)
    subprocess.run("python3 service.py {f} {e} {t} {r}"
                   .format(f=data_file_name, e=experiment, t=tasks_types, r=results), shell=True)
    # Chains count
    chains_df = pd.read_parquet(chains_file)
    chains_count = len(chains_df)
    del chains_df
    # os.remove(chains_file)
    scaffolds_files = os.listdir(scaffolds_path1)
    sub_graphs_files = os.listdir(sub_graphs_path1)
    scaffolds_count, sub_graphs_count = len(scaffolds_files), len(sub_graphs_files)
    scaffolds_sizes = []
    for scaffolds_file in scaffolds_files:
        scaffold_path = os.path.join(scaffolds_path, scaffolds_file)
        scaffold_dict = np.load(scaffold_path, allow_pickle=True)[()]
        scaffolds_sizes.append(len(scaffold_dict))
    mean_scaffolds = np.mean(np.array(scaffolds_sizes))

    nodes_sizes, edges_sizes = [], []
    for sub_graphs_file in sub_graphs_files:
        sub_graph_path = os.path.join(sub_graphs_path, sub_graphs_file)
        sub_graph = nx.read_edgelist(sub_graph_path, create_using=nx.DiGraph)
        nodes_size = len(list(sub_graph.nodes()))
        edges_size = len(list(sub_graph.edges()))
        nodes_sizes.append(nodes_size)
        edges_sizes.append(edges_size)
    mean_nodes = np.mean(np.array(nodes_sizes))
    mean_edges = np.mean(np.array(nodes_sizes))

    pids = open('process_ids.txt').read().split('\n')
    pids_count, unique_pids_count = len(pids), len(set(pids))
    # # Count rows
    # rows_count = 0
    # results_files = os.listdir(chunks_path)
    # print('results_files:', results_files)
    # for file in results_files:
    #     file_path = os.path.join(chunks_path, file)
    #     df = pd.read_parquet(file_path)
    #     rows_count += len(df)
    #     print('{f} count ='.format(f=file), rows_count)
    # del df
    # counts.append((i+1, chains_count, rows_count))
    # counts_df = pd.DataFrame(counts, columns=['run', 'chains', 'rows_count'])
    # counts.append((i + 1, chains_count, sub_graphs_count, scaffolds_count,\
    # mean_scaffolds, mean_nodes, mean_edges))
    counts.append((i + 1, chains_count, pids_count, unique_pids_count))
    #counts_df = pd.DataFrame(counts, columns=['run', 'chains', 'sub_graphs', 'scaffolds', \
    #                                          'mean_scaffolds', 'mean_nodes', 'mean_edges'])
    counts_df = pd.DataFrame(counts, columns=['run', 'chains', 'pids_count', 'unique_pids_count'])
    print(counts_df)
    counts_df.to_excel('test_counts.xlsx', index=False)
    os.remove('process_ids.txt')