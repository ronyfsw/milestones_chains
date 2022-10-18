from modules.config import *

def service_manager(instance_name, data_file_name, experiment, tasks_types, results, query):

    '''
    Start the compute instance, upload the data to S3, execute the service,
    stop the compute instances and then download the reuslts
    :param instance_name(str): The name of the EC2 instance running the service
    :param data_file_name(str): The name of the data file as stored in the .data directory
    :param experiment(str): The name of the experiment/run (default: The file name)
    :param tasks_types(str): The task types to include in the results ('tdas', 'milestones').
    If 'tdas' is given the chains will include both tdas and milestones
    :param results(str): The types of results to produce ('chains', 'prt'). If 'chains' is given,
    only the chains will be produced, and if 'prt' is given the chains will also be produced in
    the tabular prt format.
    :param query(str): The anticipated mode of querying the results ('web', 'script'). The recommended mode
    is using a script but if 'web' is given, the results will be zipped in way that will allow them to
    be queried by the web-shell of DuckDB
    '''

    start_time = datetime.now().strftime("%H:%M:%S")
    print('service started on', start_time)

    ## Compute instance
    # Set up
    INSTANCE_IP = INSTANCE_IPs[instance_name]
    INSTANCE_ID = INSTANCE_IDs[instance_name]
    EC2_RESOURCE = boto3.resource('ec2', region_name=AWS_REGION)
    EC2_CLIENT = boto3.client('ec2', region_name=AWS_REGION)

    # Start
    instance = EC2_RESOURCE.Instance(INSTANCE_ID)
    instance.start()
    print('EC2 instance {n} {id} started'.format(n=instance_name, id=INSTANCE_ID))
    instance.wait_until_running()
    print('The instance is running')

    # SSH Connector
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    privkey = paramiko.RSAKey.from_private_key_file('ds_eu_west2_2.pem')  # Worked only with local pem file
    ssh.connect(hostname=INSTANCE_IP, username='ubuntu', pkey=privkey)

    ## Calculation
    # Directories, files and paths
    working_dir = os.getcwd()
    if experiment in os.listdir(working_dir):
        shutil.rmtree(experiment)
    os.mkdir(experiment)
    data_path = os.path.join(working_dir, 'data', data_file_name)
    chains_file = 'chains.parquet'
    chains_path = os.path.join(experiment, chains_file)
    bucket_chains_path = '{e}/{c}'.format(e=experiment, c=chains_file)
    prt_path = os.path.join(experiment, 'prt')
    spreadsheet = os.path.join(experiment, 'results.xlsx')
    zipped_parquet_files = os.path.join(experiment, 'results.parquet')


    # Upload data to an S3 bucket
    S3_CLIENT.upload_file(data_path, data_bucket, data_file_name)
    print('Data file uploaded to S3')

    # Create experiment directory on S3
    S3_CLIENT.put_object(Bucket=results_bucket, Key=(experiment + '/'))

    # Run calculation
    process_statement = 'cd services/milestones_chains && python3 service.py {i} {f} {e} {t} {r}'\
    .format(i=instance_name, f=data_file_name, e=experiment, t=tasks_types, r=results)
    stdin, stdout, stderr = ssh.exec_command(process_statement)
    if len(stderr.readlines()) > 0:
        print('Run attempt encountered error:', stderr.readlines())
    print('Calculation finished')

    # Stop compute instance
    response = EC2_CLIENT.stop_instances(InstanceIds=[INSTANCE_ID], DryRun=False)
    print('EC2 instance stopped')

    # Prepare results
    print('Preparing results')
    print('bucket_chains_path, chains_path:', bucket_chains_path, chains_path)
    S3_RESOURCE.Bucket(results_bucket).download_file(bucket_chains_path, chains_path)
    rows_count = 0
    if results == 'prt':
        S3_RESOURCE.Bucket(results_bucket).download_file(prt_path, 'prt')
        with ZipFile('prt', 'r') as zipObj:
            zipObj.extractall(path=prt_path)
        os.remove('prt')

        # Write the results to an MS Excel spreadsheet if the calculation produced less than 100K chains
        prt_files = os.listdir(prt_path)
        print('prt_files:', prt_files)
        prt_files_count = len(prt_files)
        if prt_files_count == 1:
            file_path = os.path.join(prt_path, prt_files[0])
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
            pq.write_table(pq.ParquetDataset(prt_path).read(), zipped_parquet_files, row_group_size=100000)
    chains_df = pd.read_parquet(chains_path)
    chains_count = len(chains_df)
    print('{c} chains written to {f}'.format(c=chains_count, f='chains.parquet'))
    if rows_count > 0:
        print('{r} tasks rows written to {c} parquet files in the prt sub-directory'
              .format(c=prt_files_count, r=rows_count))

    # Remove result files from instance
    print('service started on', start_time)
    print('service ended on', datetime.now().strftime("%H:%M:%S"))