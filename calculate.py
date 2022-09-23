from client_set_up import *

def run_calculation_process(data_file_name, experiment, tasks_types, results, query):
    # Experiment directories
    working_dir = os.getcwd()
    if experiment in os.listdir(working_dir):
        shutil.rmtree(experiment)
    os.mkdir(experiment)
    data_path = os.path.join(working_dir, 'data', data_file_name)

    # Experiment results file
    spreadsheet = os.path.join(experiment, 'results.xlsx')
    zipped_parquet_files = os.path.join(experiment, 'results.txt')
    chains_file = os.path.join(experiment, 'chains.parquet')
    chains_list = os.path.join(experiment, 'chains.txt')

    # Upload data to an S3 bucket
    S3_CLIENT.upload_file(data_path, data_bucket, data_file_name)
    print('Data file uploaded to S3')
    # Run calculation
    process_statement = 'cd services/milestones_chains && python3 service.py {f} {e} {t} {r}'\
    .format(f=data_file_name, e=experiment, t=tasks_types, r=results)
    stdin, stdout, stderr = ssh.exec_command(process_statement)
    if stderr.readlines():
        print('Run attempt encountered error:\n', stderr.readlines())
    print('Calculation finished')
    # Stop compute instance
    response = EC2_CLIENT.stop_instances(InstanceIds=[INSTANCE_ID], DryRun=False)
    print('Compute instance stopped')
    # Prepare results
    print('Preparing results')
    S3_RESOURCE.Bucket(results_bucket).download_file(experiment, 'tabular_result_files')
    if results == 'chains':
        print('downloading chains parquet file')
        S3_RESOURCE.Bucket(results_bucket).download_file(chains_file, chains_file)
        print('downloading chains list file')
        S3_RESOURCE.Bucket(results_bucket).download_file(chains_list, chains_list)
    with ZipFile('tabular_result_files', 'r') as zipObj:
        zipObj.extractall(path=experiment)
    shutil.rmtree('tabular_result_files')
    results_files = os.listdir(experiment)
    # Write the results to an MS Excel spreadsheet if the calculation produced less than 100K chains
    if len(results_files) == 1:
        file_path = os.path.join(experiment, results_files[0])
        df = pd.read_parquet(file_path)
        df.to_excel(spreadsheet, index=False)
    # Zip the parquet files if the user intends to use the web interface
    if query == 'web':
        pq.write_table(pq.ParquetDataset(experiment).read(), zipped_parquet_files, row_group_size=100000)
    print('The results have been downloaded to {e}'.format(e=experiment))
