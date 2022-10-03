from modules.config import *
run_dir_path = os.path.join(os.getcwd(), 'run_dir_ems')
scaffolds_path1 = os.path.join(run_dir_path, 'scaffolds')
scaffolds_files = os.listdir(scaffolds_path1)
scaffolds_count = len(scaffolds_files)
nodes_decoder = np.load(os.path.join(run_dir_path, 'nodes_decoder.npy'), allow_pickle=True)[()]

scaffolds_sizes = []
for scaffolds_file in scaffolds_files:
    scaffold_path = os.path.join(scaffolds_path1, scaffolds_file)
    scaffold = np.load(scaffold_path, allow_pickle=True)[()]
    print(scaffolds_file, len(scaffold))
    #for k, chain in scaffold.items():
