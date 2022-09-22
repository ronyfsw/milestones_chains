# import os, sys, pathlib
# modules_dir = os.path.join(pathlib.Path.home(), 'services/milestones_chains/modules/')
# if modules_dir not in sys.path: sys.path.append(modules_dir)
from modules.libraries import *

# Storage
profile_name = 'ds_sandbox'
data_bucket = 'programsdatabucket'
results_bucket = 'chainsresults'
session = boto3.session.Session(profile_name=profile_name)
S3_CLIENT = session.client('s3')
S3_RESOURCE = boto3.resource('s3')

## Compute instance
# Parameters
resultsIP = '172.31.10.240'
serviceIP = '172.31.15.123'
INSTANCE_ID = 'i-0586e11281d4b02a2'
AWS_REGION = "eu-west-2"
EC2_RESOURCE = boto3.resource('ec2', region_name=AWS_REGION)
EC2_CLIENT = boto3.client('ec2', region_name=AWS_REGION)

# Start compute instance
instance = EC2_RESOURCE.Instance(INSTANCE_ID)
instance.start()
print(f'Starting EC2 instance: {instance.id}')
instance.wait_until_running()
print(f'EC2 instance "{instance.id}" has been started')
# Connect to instance to run process
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
privkey = paramiko.RSAKey.from_private_key_file('ds_eu_west2_2.pem') # Works only with local pem file
ssh.connect(hostname=serviceIP, username='ubuntu', pkey=privkey)