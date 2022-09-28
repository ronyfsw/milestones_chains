import os

from modules.config import *

data_file_name = 'EMS_DCMA_DD_23.08.graphml'
experiment = None
tasks_types = 'milestones'
results = 'prt'
query = 'code'
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
    os.remove(chains_file)
    # Count rows
    rows_count = 0
    results_files = os.listdir(chunks_path)
    print('results_files:', results_files)
    for file in results_files:
        file_path = os.path.join(chunks_path, file)
        df = pd.read_parquet(file_path)
        rows_count += len(df)
        print('{f} count ='.format(f=file), rows_count)
    del df
    counts.append((i+1, chains_count, rows_count))
    counts_df = pd.DataFrame(counts, columns=['run', 'chains', 'rows_count'])
    print(counts_df)
counts_df.to_excel('chains_rows_counts.xlsx', index=False)