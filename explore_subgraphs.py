from modules.config import *
run_dir_path = os.path.join(os.getcwd(), 'run_dir')
sub_graphs_path = os.path.join(run_dir_path, 'sub_graphs')
sub_graphs_files = os.listdir(sub_graphs_path)
sub_graphs_count = len(sub_graphs_files)
nodes_decoder = np.load(os.path.join(run_dir_path, 'nodes_decoder.npy'), allow_pickle=True)[()]

sub_graphs_sizes = []
for sub_graphs_file in sub_graphs_files:
    sub_graph_path = os.path.join(sub_graphs_path, sub_graphs_file)
    sub_graph = nx.read_edgelist(sub_graph_path, create_using=nx.DiGraph)
    Gsort = list(nx.topological_sort(sub_graph))
    root_node = Gsort[0]
    print(sub_graphs_file, len(sub_graph), root_node)
