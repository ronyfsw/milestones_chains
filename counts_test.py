import os

from modules.config import *
run_dir_path = os.path.join(os.getcwd(), 'run_dir')
scaffolds_path1 = os.path.join(run_dir_path, 'scaffolds')
sub_graphs_path1 = os.path.join(run_dir_path, 'sub_graphs')
terminals_path = os.path.join(run_dir_path, 'terminal_nodes.txt')
root_node = 'MI20-EMS-N2-60000'
data_file_name = 'EMS_DCMA_DD_23.08.graphml'
experiment = None
tasks_types = 'milestones'
results = 'no'
if not experiment: experiment = data_file_name.replace('.graphml', '').replace('.', '_')
counts = []

if 'runs_chains' in os.listdir():
    shutil.rmtree('runs_chains')
os.mkdir('runs_chains')

if 'run_dir' in os.listdir():
    shutil.rmtree('run_dir')

if 'error_terminals.txt' in os.listdir():
    os.remove('error_terminals.txt')

for i in range(30):
    print('run', i+1)
    subprocess.run("python3 service.py {f} {e} {t} {r}"
                   .format(f=data_file_name, e=experiment, t=tasks_types, r=results), shell=True)
    print('subprocess running service finished')
    # Chains count
    chains_df = pd.read_parquet(chains_file)
    chains_count = len(chains_df)
    file_name = os.path.join('runs_chains', 'chains_{n}'.format(n=str(chains_count)))
    chains = list(chains_df['Chain'].unique())
    chains_df.to_parquet(file_name)

    del chains_df
    # os.remove(chains_file)
    # scaffolds_files = os.listdir(scaffolds_path1)
    # sub_graphs_files = os.listdir(sub_graphs_path1)
    # scaffolds_count, sub_graphs_count = len(scaffolds_files), len(sub_graphs_files)
    # scaffolds_sizes = []
    # for scaffolds_file in scaffolds_files:
    #     scaffold_path = os.path.join(scaffolds_path, scaffolds_file)
    #     scaffold_dict = np.load(scaffold_path, allow_pickle=True)[()]
    #     scaffolds_sizes.append(len(scaffold_dict))
    # mean_scaffolds = np.mean(np.array(scaffolds_sizes))
    #
    # nodes_sizes, edges_sizes = [], []
    # for sub_graphs_file in sub_graphs_files:
    #     sub_graph_path = os.path.join(sub_graphs_path, sub_graphs_file)
    #     sub_graph = nx.read_edgelist(sub_graph_path, create_using=nx.DiGraph)
    #     nodes_size = len(list(sub_graph.nodes()))
    #     edges_size = len(list(sub_graph.edges()))
    #     nodes_sizes.append(nodes_size)
    #     edges_sizes.append(edges_size)
    # mean_nodes = np.mean(np.array(nodes_sizes))
    # mean_edges = np.mean(np.array(nodes_sizes))

    # pids = open('process_ids.txt').read().split('\n')
    # pids_count, unique_pids_count = len(pids), len(set(pids))

    # Terminal nodes
    error_terminals = []
    nodes_decoder = np.load(os.path.join(run_dir_path, 'nodes_decoder.npy'), allow_pickle=True)[()]
    terminal_nodes = open(terminals_path).read().split('\n')
    terminal_nodes = [nodes_decoder[n] for n in terminal_nodes]
    root_errors = terminal_errors = 0
    for chain in chains:
        tasks = chain.split(node_delimiter)
        chain_root, chain_terminal = tasks[0], tasks[-1]
        # print(chain_root, chain_terminal)
        if chain_root != root_node: root_errors += 1
        if chain_terminal not in terminal_nodes:
            terminal_errors += 1
            error_terminals.append(chain_terminal)
    chains_count = len(chains)
    root_errors_rate = 100 * (root_errors / chains_count)
    terminal_errors_rate = 100 * (terminal_errors / chains_count)
    print('*** error terminals ***\n', error_terminals)
    with open('error_terminals.txt', 'a') as f: f.write('\n'.join(error_terminals)+'\n')

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
    #counts_df = pd.DataFrame(counts, columns=['run', 'chains', 'sub_graphs', 'scaffolds', \
    #                                          'mean_scaffolds', 'mean_nodes', 'mean_edges'])
    #counts.append((i + 1, chains_count, pids_count, unique_pids_count))
    #counts_df = pd.DataFrame(counts, columns=['run', 'chains', 'pids_count', 'unique_pids_count'])
    counts.append((i + 1, chains_count, root_errors_rate, terminal_errors_rate))
    counts_df = pd.DataFrame(counts, columns=['run', 'chains', 'root_errors_rate', 'terminal_errors_rate'])

    print(counts_df)
    counts_df.to_excel('test_counts.xlsx', index=False)
    #os.remove('process_ids.txt')
