import os
from modules.config import *
source_path = '/home/rony/services/milestones_chains/data/EMS_DCMA_DD/predecessors_sueccessors.xlsx'
data_file_name = 'EMS_DCMA_DD_23.08.graphml'
experiment = None
tasks_types = 'milestones'
results = 'no'
if not experiment: experiment = data_file_name.replace('.graphml', '').replace('.', '_')
if 'runs_chains' in os.listdir(): shutil.rmtree('runs_chains')
os.mkdir('runs_chains')
if 'run_dir' in os.listdir():shutil.rmtree('run_dir')

counts = []
for i in range(30):
    print('run', i+1)
    subprocess.run("python3 service.py {f} {e} {t} {r}"
                   .format(f=data_file_name, e=experiment, t=tasks_types, r=results), shell=True)
    print('subprocess running service finished')
    chains_df = pd.read_parquet(chains_file)
    chains = list(chains_df['Chain'].unique())

    # Chains count
    chains_count = len(chains_df)
    file_name = os.path.join('runs_chains', 'chains_{n}'.format(n=str(chains_count)))
    chains_df.to_parquet(file_name)
    del chains_df

    counts.append((i + 1, chains_count))
    counts_df = pd.DataFrame(counts, columns=['run', 'chains'])

    print(counts_df)
    counts_df.to_excel('test_counts.xlsx', index=False)
    #os.remove('process_ids.txt')
