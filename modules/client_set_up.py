import os, sys, pathlib
modules_dir = os.path.join(pathlib.Path.home(), 'services/milestones_chains/modules/')
if modules_dir not in sys.path: sys.path.append(modules_dir)
from libraries import *

# Storage
profile_name = 'ds_sandbox'
data_bucket = 'programsdatabucket'
results_bucket = 'chainsresults'
session = boto3.session.Session(profile_name=profile_name)
S3_CLIENT = session.client('s3')
S3_RESOURCE = boto3.resource('s3')

## Compute instance
# Parameters
INSTANCE_NAME = 'service'
INSTANCE_NAME = 'service_dev'
INSTANCE_IPs = {'service': '172.31.15.123', 'service_dev': '172.31.20.61'}
INSTANCE_IDs = {'service': 'i-0586e11281d4b02a2', 'service_dev': 'i-0249408ea16bc730b'}
INSTANCE_IP = INSTANCE_IPs[INSTANCE_NAME]
INSTANCE_ID = INSTANCE_IDs[INSTANCE_NAME]
AWS_REGION = "eu-west-2"
EC2_RESOURCE = boto3.resource('ec2', region_name=AWS_REGION)
EC2_CLIENT = boto3.client('ec2', region_name=AWS_REGION)

# Start compute instance
instance = EC2_RESOURCE.Instance(INSTANCE_ID)
instance.start()
print(f'Starting EC2 instance: {instance.id}')
instance.wait_until_running()
print('EC2 instance {n} {id} started'.format(n=INSTANCE_NAME, id=INSTANCE_ID))

# Connect to instance to run process
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
privkey = paramiko.RSAKey.from_private_key_file('ds_eu_west2_2.pem') # Worked only with local pem file
ssh.connect(hostname=INSTANCE_IP, username='ubuntu', pkey=privkey)