import os
import networkx as nx
import boto3
import paramiko

# Run arguments
host_os = 'linux_mac' # or 'windows'
data_dir = '/home/rony/services/milestones_chains/data/'
data_file_name = 'EMS_DCMA_DD_23.08.graphml'
data_path = os.path.join(data_dir, data_file_name)
experiment = 'EMS_DCMA_DD_23_08'
tasks = 'tdas'
results = 'prt'

## AWS
# IPs
resultsIP = '172.31.10.240'
serviceIP = '172.31.15.123'
# Compute instance
key_file_name = 'ds_eu_west2_2.pem'
if host_os=='windows':
    key_file_name = 'C:\\Users\\username\\.aws\\ds_eu_west2_2.pem'
else:
    key_file_name = '~/.aws/ds_eu_west2_2.pem'
INSTANCE_ID = 'i-0586e11281d4b02a2'
AWS_REGION = "eu-west-2"
EC2_RESOURCE = boto3.resource('ec2', region_name=AWS_REGION)
EC2_CLIENT = boto3.client('ec2', region_name=AWS_REGION)

# Storage bucket
profile_name = 'ds_sandbox'
data_bucket = 'programsdatabucket'
results_bucket = 'chainsresults'
session = boto3.session.Session(profile_name=profile_name)
s3_client = session.client('s3')
s3_resource = boto3.resource('s3')

# Start compute instance
instance = EC2_RESOURCE.Instance(INSTANCE_ID)
print('instance:', instance)
instance.start()
print(f'Starting EC2 instance: {instance.id}')
instance.wait_until_running()
print(f'EC2 instance "{instance.id}" has been started')

# Connect to instance to run process 
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
privkey = paramiko.RSAKey.from_private_key_file(key_file_name) # Works only with local pem file
ssh.connect(hostname=serviceIP, username='ubuntu', pkey=privkey)
