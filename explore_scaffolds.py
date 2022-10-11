from modules.config import *
scaffolds_path = '/home/rony/services/milestones_chains/run_dir/scaffolds'
scaffolds_files = os.listdir(scaffolds_path)
scaffolds_count = len(scaffolds_files)

scaffolds_sizes = []
for scaffolds_file in scaffolds_files:
    scaffold_path = os.path.join(scaffolds_path, scaffolds_file)
    scaffold = np.load(scaffold_path, allow_pickle=True)[()]
    print(scaffolds_file, len(scaffold))
    for k, chain in scaffold.items():
        print(k, chain)
