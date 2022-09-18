from pathlib import Path
import os
import sys
home_dir = Path.home()
modules_dir = os.path.join(home_dir, 'services/milestones_chains/modules/')
if modules_dir not in sys.path: sys.path.append(modules_dir)
from config import *

parser = argparse.ArgumentParser()
parser.add_argument('experiment')
args = parser.parse_args()
experiment = args.experiment
zipped_results_file_name = '{e}_results.zip'.format(e=experiment)

start_time = datetime.now().strftime("%H:%M:%S")
print('merge and upload results started on', start_time)

start = time.time()
print('combine results')
# pq.write_table(pq.ParquetDataset(chunks_path).read(), 'results.parquet', row_group_size=100000)
print('combine results took {t} seconds'.format(t=time.time()-start))
print('zip results')
start = time.time()
# os.system('zip {r} {f}'.format(f='results.parquet', r=zipped_results_file_name))
file_paths = {}
file_names = os.listdir(chunks_path)
print('results files names:', file_names)
for file_name in file_names:
    file_paths[file_name] = os.path.join(chunks_path, file_name)
with ZipFile(zipped_results_file_name, 'w') as zip:
     for file_name, file_path in file_paths.items():
        # print(file_name) 
        zip.write(file_path, arcname=file_name)
print('zip results took {t} seconds'.format(t=time.time()-start))
print('uploading zipped file to s3')
start = time.time()
s3_client.upload_file(zipped_results_file_name, results_bucket, experiment)
print('merge and upload results ended on', datetime.now().strftime("%H:%M:%S"))
