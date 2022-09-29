import os
from modules.config import *
source_path = '/home/rony/services/milestones_chains/data/EMS_DCMA_DD/predecessor_sueccessors.xlsx'
data_file_name = 'EMS_DCMA_DD_23.08.graphml'
experiment = None
tasks_types = 'milestones'
results = 'no'
if not experiment: experiment = data_file_name.replace('.graphml', '').replace('.', '_')

source = pd.read_excel(source_path)
source_pairs = dict(zip(source['predecessor'], source['successors']))


counts = []
for i in range(30):
    print('run', i+1)
    subprocess.run("python3 service.py {f} {e} {t} {r}"
                   .format(f=data_file_name, e=experiment, t=tasks_types, r=results), shell=True)
    print('subprocess running service finished')
    chains_df = pd.read_parquet(chains_file)
    chains = list(chains_df['Chain'].unique())
    chains_count = len(chains_df)
    ps_errors = 0
    ps_errors_items = []
    for chain in chains:
        tasks = chain.split(node_delimiter)
        for index, task in enumerate(tasks):
            if index < len(tasks)-1:
                chain_successor = tasks[index+1]
                if chain_successor not in source_pairs[task]:
                    ps_errors += 1
                    ps_errors_items.append((task, chain_successor))
    ps_errors_rate = 100 * (ps_errors / chains_count)
    print('*** ps errors ***\n', ps_errors_items)

    counts.append((i + 1, chains_count, ps_errors_rate))
    counts_df = pd.DataFrame(counts, columns=['run', 'chains', 'ps_errors_rate'])

    print(counts_df)
    counts_df.to_excel('test_counts.xlsx', index=False)
    #os.remove('process_ids.txt')
