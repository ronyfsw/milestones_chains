import os, sys, pathlib
modules_dir = os.path.join(pathlib.Path.home(), 'services/milestones_chains/modules/')
if modules_dir not in sys.path: sys.path.append(modules_dir)
from client_set_up import *

def run_calculation_process(data_file_name, experiment, tasks, results, web_query=False):
    working_dir = os.getcwd()
    data_path = os.path.join(working_dir, 'data', data_file_name)
    if experiment in os.listdir(working_dir):
        shutil.rmtree(experiment)
    os.mkdir(experiment)
    # Upload data to an S3 bucket
    S3_CLIENT.upload_file(data_path, data_bucket, data_file_name)
    print('Data file uploaded to S3')
    # Run calculation
    process_statement = 'cd services/milestones_chains && python3 service.py {f} {e} {t} {r}'\
    .format(f=data_file_name, e=experiment, t=tasks, r=results)
    stdin, stdout, stderr = ssh.exec_command(process_statement)
    if stderr.readlines():
        print('Run attempt encountered error:\n', stderr.readlines())
    print('Calculation finished')
    # Stop compute instance
    response = EC2_CLIENT.stop_instances(InstanceIds=[INSTANCE_ID], DryRun=False)
    print('Compute instance stopped')

    # Prepare results
    print('Preparing results')
    S3_RESOURCE.Bucket(results_bucket).download_file(experiment, 'experiment_zipped')
    with ZipFile('experiment_zipped', 'r') as zipObj:
        zipObj.extractall(path=experiment)
    results_files = os.listdir(experiment)
    if len(results_files) == 1:
        file_path = os.path.join(experiment, results_files[0])
        df = pd.read_parquet(file_path)
        df.to_excel('{e}_results.xlsx'.format(e=experiment), index=False)

    if web_query:
        pq.write_table(pq.ParquetDataset(experiment).read(), 'results.parquet', row_group_size=100000)

    print('The results have been downloaded to {e}'.format(e=experiment))
