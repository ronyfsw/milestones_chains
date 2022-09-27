import subprocess
import time
data_file_name = 'EMS_DCMA_DD_23.08.graphml'
experiment = None
tasks_types = 'milestones'
results = 'prt'
query = 'code'
if not experiment: experiment = data_file_name.replace('.graphml', '').replace('.', '_')
counts = []

from calculate import *

for i in range(30):
    print('run', i+1)
    chains_count, rows_count = run_calculation_process(data_file_name, experiment, tasks_types, results, query)
    counts.append((i+1, chains_count, rows_count))
    counts_df = pd.DataFrame(counts, columns=['run', 'chains', 'rows_count'])
    print(counts_df)
counts_df.to_excel('counts_df.xlsx', index=False)