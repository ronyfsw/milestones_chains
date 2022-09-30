from modules.config import *
run_dir_path = os.path.join(os.getcwd(), 'run_dir')
scaffolds_path1 = os.path.join(run_dir_path, 'scaffolds')
scaffolds_files = os.listdir(scaffolds_path1)
scaffolds_count = len(scaffolds_files)
nodes_decoder = np.load(os.path.join(run_dir_path, 'nodes_decoder.npy'), allow_pickle=True)[()]

scaffolds_sizes = []
error_pair = ('MI20-EMS-N2-505856', 'MI20-EMS-N2-505860')
# Source spreadsheet: MI20-EMS-N2-505858	MI20-EMS-N2-505860

successor = 'MI20-EMS-N2-505860'
for scaffolds_file in scaffolds_files:
    scaffold_path = os.path.join(scaffolds_path, scaffolds_file)
    scaffold = np.load(scaffold_path, allow_pickle=True)[()]
    for k, chain in scaffold.items():
        encoded_tasks = chain.split(node_delimiter)
        tasks = [nodes_decoder[t] for t in encoded_tasks]
        if successor in tasks:
            index = tasks.index(successor)
            predecesor = tasks[index-1]
            #print(scaffolds_file, node_delimiter.join([predecesor, successor]))
            print(node_delimiter.join(tasks))