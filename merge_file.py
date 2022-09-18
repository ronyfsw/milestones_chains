import pandas as pd
import pyarrow.parquet as pq
import time
import os
from modules.config import *
from zipfile import ZipFile

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

s3_client.upload_file('{experiment}_results.zip'.format(e=experiment), results_bucket, experiment)
print('upload results took {t} seconds'.format(t=time.time()-start))