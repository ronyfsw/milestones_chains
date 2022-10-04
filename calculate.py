from modules.client_set_up import *

def run_calculation_process(data_file_name, experiment, tasks_types, results, query):
    # Experiment directories
    working_dir = os.getcwd()
    if experiment in os.listdir(working_dir):
        shutil.rmtree(experiment)
    os.mkdir(experiment)
    data_path = os.path.join(working_dir, 'data', data_file_name)
    chains_file = 'chains.parquet'
    chains_path = os.path.join(experiment, chains_file)
    bucket_chains_path = '{e}/{c}'.format(e=experiment, c=chains_file)
    prt_path = os.path.join(experiment, 'prt')
    bucket_prt_path = '{e}/prt'.format(e=experiment)

    # Experiment results file
    spreadsheet = os.path.join(experiment, 'results.xlsx')
    zipped_parquet_files = os.path.join(experiment, 'results.parquet')

    # Upload data to an S3 bucket
    S3_CLIENT.upload_file(data_path, data_bucket, data_file_name)
    print('Data file uploaded to S3')

    # Create experiment directory on S3
    S3_CLIENT.put_object(Bucket=results_bucket, Key=(experiment + '/'))

    # Run calculation
    stdin, stdout, stderr = ssh.exec_command('pwd')
    process_statement = 'cd services/milestones_chains && python3 service.py {f} {e} {t} {r}'\
    .format(f=data_file_name, e=experiment, t=tasks_types, r=results)
    stdin, stdout, stderr = ssh.exec_command(process_statement)
    if len(stderr.readlines()) > 0:
        print('Run attempt encountered error:', stderr.readlines())

    print('Calculation finished')
    # Stop compute instance
    response = EC2_CLIENT.stop_instances(InstanceIds=[INSTANCE_ID], DryRun=False)
    print('Compute instance stopped')
    # Prepare results
    print('Preparing results')
    S3_RESOURCE.Bucket(results_bucket).download_file(bucket_chains_path, chains_path)
    rows_count = 0
    if results == 'prt':
        S3_RESOURCE.Bucket(results_bucket).download_file(bucket_prt_path, 'prt')
        with ZipFile('prt', 'r') as zipObj:
            zipObj.extractall(path=prt_path)
        os.remove('prt')
        results_files = os.listdir(experiment)
        # Write the results to an MS Excel spreadsheet if the calculation produced less than 100K chains
        prt_files_count = len(os.listdir(prt_path))
        if prt_files_count == 1:
            file_path = os.path.join(experiment, results_files[0])
            df = pd.read_parquet(file_path)
            df.to_excel(spreadsheet, index=False)
        results_files = os.listdir(experiment)
        print('The following results files are available in the {e} directory:\n'
              .format(e=experiment), ','.join(results_files))
        # Count rows
        prt_files = os.listdir(prt_path)
        for file in prt_files:
            file_path = os.path.join(prt_path, file)
            df = pd.read_parquet(file_path)
            rows_count += len(df)
            del df
        # Zip the parquet files if the user intends to use the web interface
        if query == 'web':
            pq.write_table(pq.ParquetDataset(experiment).read(), zipped_parquet_files, row_group_size=100000)
    chains_df = pd.read_parquet(chains_path)
    chains_count = len(chains_df)
    print('{c} chains written to {f}'.format(c=chains_count, f='chains.parquet'))
    if rows_count > 0:
        print('{r} tasks rows written to parquet files in the prt sub-directory'.format(r=rows_count))